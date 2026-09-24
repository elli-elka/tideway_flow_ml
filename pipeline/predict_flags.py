"""
Scratch model: predict the PLA Ebb Tide Flag for the next 7 days (14 issues at
06:00/18:00 UK time).

What it predicts: the lowest Richmond level (m above chart datum) in each future
12h flag window, as a change from the latest one. The flag colour follows from the
PLA thresholds; probabilities come from the model's typical error at each horizon.

Inputs (each used only if its table exists and has data; missing values are fine):
  richmond_ebb_flags        recent low-water levels (built by build_ebb_flags.py)
  richmond_pla_levels       PLA astronomical tide prediction for future windows
  catchment_rain_daily      observed rain, evapotranspiration, soil moisture
  catchment_rain_forecast   rain forecast for the days ahead (as issued at the time):
                            total to the target flag, and the 5 days before it (the
                            rain that has time to reach Teddington)
  kingston_flow_readings,   river flow at Kingston and upstream gauges
  kingston_daily_flow_nrfa,
  river_flow_daily
  groundwater_daily         chalk groundwater levels
plus the spring-neap tide cycle, computed from the moon's phase.

Each run backtests three methods on the most recent 25% of history and, for each
horizon, uses whichever did best:
  same_as_now  the latest low at the same time of day (06:00 or 18:00) carries on
  ridge        linear model (steady with little data)
  gbm          gradient-boosted trees (needs more data, can learn rain/flow effects)
Predictions and the method used are saved to ebb_flag_predictions so they can be
compared with the real flags later.

To see how predictions compared with the real flags:
  SELECT horizon, AVG(abs_error_m), AVG(flag_correct::int)
  FROM ebb_flag_prediction_scores GROUP BY horizon ORDER BY horizon;

Usage: python predict_flags.py [--no-save]
"""

import math
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import psycopg
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from build_ebb_flags import FLAGS_TABLE, issue_times
from common import get_database_url


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

MODEL_VERSION = "scratch-v1"
PREDICTIONS_TABLE = "ebb_flag_predictions"
HORIZONS = range(1, 15)          # issues ahead (12h each) -> 7 days
TEST_FRACTION = 0.25
MIN_TRAIN_ISSUES = 60
# Time-of-year features can only be learned from several years of history
MIN_DAYS_FOR_SEASONAL = 730
UK = ZoneInfo("Europe/London")

THRESHOLDS = [("BLACK", -np.inf, 0.0), ("GREEN", 0.0, 1.7), ("YELLOW", 1.7, 2.6), ("RED", 2.6, np.inf)]

# Spring-neap cycle: half a synodic month, anchored on a known new moon
NEW_MOON_EPOCH = datetime(2000, 1, 6, 18, 14, tzinfo=timezone.utc)
SPRING_NEAP_DAYS = 29.530588853 / 2


# --------------------------------------------------
# UTILS
# --------------------------------------------------

def flag_for(level):
    for name, lo, hi in THRESHOLDS:
        if lo <= level < hi:
            return name


def flag_probabilities(level, sigma):
    cdf = lambda x: 0.5 * (1 + math.erf((x - level) / (sigma * math.sqrt(2)))) if np.isfinite(x) else (0.0 if x < 0 else 1.0)
    return {name: cdf(hi) - cdf(lo) for name, lo, hi in THRESHOLDS}


def spring_neap(ts):
    phase = ((ts - NEW_MOON_EPOCH).total_seconds() / 86400 / SPRING_NEAP_DAYS) % 1.0
    return math.sin(2 * math.pi * phase), math.cos(2 * math.pi * phase)


def read_table(conn, sql):
    try:
        return pd.read_sql(sql, conn)
    except Exception:
        conn.rollback()
        return pd.DataFrame()


def local_day(ts):
    return ts.astimezone(UK).date()


# --------------------------------------------------
# DATA
# --------------------------------------------------

def load(conn):
    data = {}
    data["flags"] = read_table(conn, f"SELECT issued_at, min_level_cd FROM {FLAGS_TABLE} ORDER BY issued_at")
    data["pla"] = read_table(conn, "SELECT ts, predicted FROM richmond_pla_levels WHERE predicted IS NOT NULL")
    data["rain"] = read_table(conn, """
        SELECT day, AVG(precip_mm) AS rain, AVG(et0_mm) AS et0,
               AVG(soil_moisture_deep) AS soil_deep, AVG(soil_moisture_shallow) AS soil_shallow
        FROM catchment_rain_daily GROUP BY day""")
    data["forecast"] = read_table(conn, """
        SELECT issued_date, target_day, AVG(precip_mm) AS rain
        FROM catchment_rain_forecast GROUP BY issued_date, target_day""")
    kingston = read_table(conn, """
        SELECT (ts AT TIME ZONE 'Europe/London')::date AS day, AVG(flow_m3s) AS flow
        FROM kingston_flow_readings GROUP BY 1""")
    nrfa = read_table(conn, "SELECT day, flow_m3s AS flow FROM kingston_daily_flow_nrfa")
    upstream = read_table(conn, "SELECT day, gauge, flow_m3s FROM river_flow_daily")
    ground = read_table(conn, "SELECT day, area, level_maod FROM groundwater_daily")

    # One daily frame of slow-moving catchment state
    frames = []
    k = pd.concat([nrfa, kingston]).drop_duplicates("day", keep="last") if len(kingston) or len(nrfa) else pd.DataFrame()
    if len(k):
        frames.append(k.set_index("day").rename(columns={"flow": "kingston_flow"}))
    if len(upstream):
        frames.append(upstream.pivot_table(index="day", columns="gauge", values="flow_m3s").add_prefix("flow_"))
    if len(ground):
        frames.append(ground.pivot_table(index="day", columns="area", values="level_maod").add_prefix("gw_"))
    if len(data["rain"]):
        frames.append(data["rain"].set_index("day"))
    daily = pd.concat(frames, axis=1) if frames else pd.DataFrame()
    if len(daily):
        daily.index = pd.to_datetime(daily.index).date
        daily = daily.sort_index()
    data["daily"] = daily
    return data


def daily_features(daily, day):
    """Catchment state as known at the start of `day` (i.e. up to the day before)."""
    if daily is None or not len(daily):
        return {}
    past = daily[daily.index < day]
    if not len(past):
        return {}
    f = {}
    if "rain" in past:
        for n in (1, 3, 7, 14, 30):
            window = past["rain"].iloc[-n:]
            f[f"rain_{n}d"] = window.sum() if window.notna().any() else np.nan
    if "et0" in past:
        f["et0_30d"] = past["et0"].iloc[-30:].sum() if past["et0"].notna().any() else np.nan
    for col in past.columns:
        if col.startswith(("flow_", "gw_", "soil_")) or col == "kingston_flow":
            series = past[col].dropna()
            if len(series) and (day - series.index[-1]).days <= 10:
                f[col] = series.iloc[-1]
                if col in ("kingston_flow",) or col.startswith("flow_"):
                    prev = series[series.index <= series.index[-1] - timedelta(days=3)]
                    f[f"{col}_chg3d"] = series.iloc[-1] - prev.iloc[-1] if len(prev) else np.nan
    return f


def rain_window(data, issue_day, start, end):
    """Catchment-mean rain from `start` to `end` (inclusive) as it was known on
    `issue_day`: observed rain for days before the issue day, and the rain forecast
    issued on (or just before) the issue day for the days after. Where no archived
    forecast exists (older training rows) observed rain stands in, which is a perfect
    forecast and so optimistic. Returns (mm, share of forecast days that were real
    forecasts)."""
    if end < start:
        return 0.0, np.nan
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    daily = data["daily"]
    observed = daily["rain"] if len(daily) and "rain" in daily else pd.Series(dtype=float)

    forecast = {}
    fc = data["forecast"]
    if len(fc):
        issued = fc[(fc.issued_date <= issue_day) & (fc.target_day >= issue_day) & (fc.target_day <= end)]
        if len(issued):
            latest = issued[issued.issued_date == issued.issued_date.max()]
            forecast = dict(zip(latest.target_day, latest.rain))

    total, future_days, real_forecasts = 0.0, 0, 0
    for day in days:
        if day >= issue_day:
            future_days += 1
            if day in forecast:
                total += forecast[day]
                real_forecasts += 1
                continue
        value = observed.get(day, np.nan)
        if pd.isna(value):
            return np.nan, np.nan
        total += value
    return total, (real_forecasts / future_days if future_days else np.nan)


def rain_ahead(data, issue_day, target_day):
    """Forecast rain from the issue day up to the target day."""
    return rain_window(data, issue_day, issue_day, target_day)


def predicted_tide_low(pla, end):
    if not len(pla):
        return np.nan
    window = pla[(pla.ts > end - timedelta(hours=12)) & (pla.ts <= end)]
    return window.predicted.min() if len(window) else np.nan


def base_features(flags, i, data):
    t = flags.issued_at.iloc[i]
    lows = flags.min_level_cd.iloc[max(0, i - 13):i + 1]
    f = {
        "low_now": lows.iloc[-1],
        "low_prev": lows.iloc[-2] if len(lows) > 1 else np.nan,
        "low_mean_2d": lows.iloc[-4:].mean(),
        "low_min_7d": lows.min(),
        "low_max_7d": lows.max(),
        "low_trend_2d": lows.iloc[-1] - lows.iloc[-5] if len(lows) > 4 else np.nan,
        "doy_sin": math.sin(2 * math.pi * t.timetuple().tm_yday / 365.25),
        "doy_cos": math.cos(2 * math.pi * t.timetuple().tm_yday / 365.25),
        "tide_pred_low_now": predicted_tide_low(data["pla"], t),
    }
    f["sn_sin_now"], f["sn_cos_now"] = spring_neap(t)
    f.update(daily_features(data["daily"], local_day(t)))
    return f


def target_features(t, target, h, data):
    sn_sin, sn_cos = spring_neap(target)
    rain, rain_is_forecast = rain_ahead(data, local_day(t), local_day(target))
    # Rain takes roughly 1-5 days to reach Teddington, so the rain that matters most
    # for a flag is what falls in the days just before it
    lagged, _ = rain_window(data, local_day(t), local_day(target) - timedelta(days=5),
                            local_day(target) - timedelta(days=1))
    return {
        "horizon": h,
        "sn_sin": sn_sin,
        "sn_cos": sn_cos,
        "target_is_morning": 1.0 if target.astimezone(UK).hour < 12 else 0.0,
        "tide_pred_low": predicted_tide_low(data["pla"], target),
        "rain_ahead": rain,
        "rain_5d_before_target": lagged,
        "rain_ahead_is_forecast": rain_is_forecast,
    }


def anchor(base, h):
    """Latest low from the same time of day as the target: morning and evening lows
    differ (the tide's daily inequality), so this is the natural starting point."""
    return base["low_now"] if h % 2 == 0 else base["low_prev"]


def build_training(data):
    flags = data["flags"]
    rows = []
    for i in range(len(flags)):
        t = flags.issued_at.iloc[i]
        base = None
        for h in HORIZONS:
            j = i + h
            if j >= len(flags):
                break
            target = flags.issued_at.iloc[j]
            # Only use consecutive issues (no missing data in between)
            if abs((target - t) - timedelta(hours=12 * h)) > timedelta(hours=2):
                break
            base = base or base_features(flags, i, data)
            a = anchor(base, h)
            if pd.isna(a):
                continue
            rows.append({"issued_at": t, "target_at": target, **base,
                         **target_features(t, target, h, data),
                         "anchor": a, "y": flags.min_level_cd.iloc[j] - a,
                         "target_level": flags.min_level_cd.iloc[j]})
    return pd.DataFrame(rows)


# --------------------------------------------------
# MODEL
# --------------------------------------------------

METHODS = ("same_as_now", "ridge", "gbm")


def make_model(method):
    if method == "ridge":
        return make_pipeline(SimpleImputer(), StandardScaler(), Ridge(alpha=10.0))
    return HistGradientBoostingRegressor(max_depth=3, learning_rate=0.05, max_iter=300,
                                         l2_regularization=1.0, min_samples_leaf=20, random_state=0)


def fit_models(train, features):
    return {m: make_model(m).fit(train[features], train.y) for m in METHODS if m != "same_as_now"}


def predict(models, method, x, anchors):
    if method == "same_as_now":
        return np.asarray(anchors, dtype=float)
    return models[method].predict(x) + np.asarray(anchors, dtype=float)


def evaluate(df, features):
    """Train on the older 75% of issues, test every method on the most recent 25%,
    and pick the best method per horizon by mean absolute error."""
    issues = np.sort(df.issued_at.unique())
    cut = issues[int(len(issues) * (1 - TEST_FRACTION))]
    train, test = df[df.issued_at < cut], df[df.issued_at >= cut]
    features = [c for c in features if train[c].notna().sum() >= 2 and train[c].nunique() > 1]
    models = fit_models(train, features)
    truth = test.target_level.map(flag_for)

    rows = []
    for h, g in test.groupby("horizon"):
        r = {"horizon": h, "n": len(g)}
        for m in METHODS:
            pred = predict(models, m, g[features], g.anchor)
            r[f"mae_{m}"] = np.abs(pred - g.target_level).mean()
            r[f"acc_{m}"] = (pd.Series(pred).map(flag_for).values == truth[g.index].values).mean()
            r[f"sigma_{m}"] = max(np.std(pred - g.target_level), 0.05)
        r["method"] = min(METHODS, key=lambda m: r[f"mae_{m}"])
        r["sigma"] = r[f"sigma_{r['method']}"]
        rows.append(r)
    return pd.DataFrame(rows).set_index("horizon"), cut, len(train), len(test)


def main():
    save = "--no-save" not in sys.argv
    database_url = get_database_url()

    with psycopg.connect(database_url) as conn:
        data = load(conn)
        flags = data["flags"]
        if len(flags) < MIN_TRAIN_ISSUES:
            print(f"Not enough flag history yet ({len(flags)} issues, need {MIN_TRAIN_ISSUES}). "
                  f"Run build_ebb_flags.py after ingesting Richmond levels.")
            return
        flags["issued_at"] = pd.to_datetime(flags.issued_at, utc=True)
        if len(data["pla"]):
            data["pla"]["ts"] = pd.to_datetime(data["pla"].ts, utc=True)
        for col in ("issued_date", "target_day"):
            if len(data["forecast"]):
                data["forecast"][col] = pd.to_datetime(data["forecast"][col]).dt.date

        df = build_training(data)
        features = [c for c in df.columns if c not in ("issued_at", "target_at", "y", "target_level", "anchor")
                    and df[c].notna().any()]
        if (flags.issued_at.max() - flags.issued_at.min()).days < MIN_DAYS_FOR_SEASONAL:
            features = [c for c in features if not c.startswith("doy_")]
        print(f"Training rows: {len(df)} from {df.issued_at.nunique()} issues "
              f"({df.issued_at.min():%Y-%m-%d} -> {df.issued_at.max():%Y-%m-%d})")
        print(f"Features ({len(features)}): {', '.join(features)}")
        rain_fc_share = df.rain_ahead_is_forecast.mean() if df.rain_ahead_is_forecast.notna().any() else None
        if rain_fc_share is not None and rain_fc_share < 1:
            print(f"NOTE: {1 - rain_fc_share:.0%} of rows use observed rain in place of an archived "
                  f"forecast, so scores are optimistic. Run backfill_rain_forecasts.py.")

        by_h, cut, n_train, n_test = evaluate(df, features)
        print(f"\nBacktest: trained on issues before {cut:%Y-%m-%d} ({n_train} rows), "
              f"tested on {n_test} rows after")
        print("  days   MAE (m): same_as_now  ridge    gbm    | flag accuracy: same_as_now ridge  gbm  | using")
        for h, r in by_h.iterrows():
            print(f"  {h / 2:>4.1f}   {r.mae_same_as_now:>20.3f} {r.mae_ridge:>6.3f} {r.mae_gbm:>6.3f}"
                  f"    | {r.acc_same_as_now:>25.0%} {r.acc_ridge:>5.0%} {r.acc_gbm:>4.0%}  | {r.method}")

        # Fit on everything and forecast from the latest issue
        features = [c for c in features if df[c].notna().sum() >= 2 and df[c].nunique() > 1]
        models = fit_models(df, features)
        i_last = len(flags) - 1
        t_last = flags.issued_at.iloc[i_last]
        age = datetime.now(timezone.utc) - t_last
        if age > timedelta(hours=18):
            print(f"\nWARNING: latest flag issue is {t_last:%Y-%m-%d %H:%M} UTC ({age.days} days old); "
                  f"forecast starts from there. Check the Richmond ingest is running.")
        base = base_features(flags, i_last, data)
        targets = list(issue_times(t_last.to_pydatetime(), t_last.to_pydatetime() + timedelta(days=7, hours=1)))
        preds = []
        for h, target in enumerate(targets[:len(HORIZONS)], start=1):
            x = pd.DataFrame([{**base, **target_features(t_last, target, h, data)}]).reindex(columns=features)
            method = by_h.method.get(h, "same_as_now")
            level = float(predict(models, method, x, [anchor(base, h)])[0])
            sigma = float(by_h.sigma.get(h, by_h.sigma.max()))
            probs = flag_probabilities(level, sigma)
            preds.append((t_last, target, h, level, probs, flag_for(level), method))

        print(f"\nForecast from the {t_last.astimezone(UK):%a %d %b %H:%M} flag "
              f"(current low {base['low_now']:.2f} m -> {flag_for(base['low_now'])}):")
        print("  issue (UK)         level   flag     P(black) P(green) P(yellow) P(red)  method")
        for _, target, h, level, probs, flag, method in preds:
            print(f"  {target.astimezone(UK):%a %d %b %H:%M}   {level:>5.2f}   {flag:<7}"
                  f"  {probs['BLACK']:>7.0%} {probs['GREEN']:>8.0%} {probs['YELLOW']:>9.0%} {probs['RED']:>6.0%}"
                  f"  {method}")

        if save:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {PREDICTIONS_TABLE} (
                        base_issue TIMESTAMPTZ NOT NULL,
                        target_issue TIMESTAMPTZ NOT NULL,
                        horizon INTEGER NOT NULL,
                        model_version TEXT NOT NULL,
                        pred_level_cd DOUBLE PRECISION NOT NULL,
                        flag TEXT NOT NULL,
                        method TEXT,
                        p_black DOUBLE PRECISION, p_green DOUBLE PRECISION,
                        p_yellow DOUBLE PRECISION, p_red DOUBLE PRECISION,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (base_issue, target_issue, model_version)
                    );
                    ALTER TABLE {PREDICTIONS_TABLE} ADD COLUMN IF NOT EXISTS method TEXT;
                    -- Each prediction next to the real flag, once that flag has been issued
                    CREATE OR REPLACE VIEW ebb_flag_prediction_scores AS
                    SELECT p.base_issue, p.target_issue, p.horizon, p.model_version, p.method,
                           p.pred_level_cd, p.flag AS pred_flag,
                           f.min_level_cd AS actual_level_cd, f.flag AS actual_flag,
                           ABS(p.pred_level_cd - f.min_level_cd) AS abs_error_m,
                           p.flag = f.flag AS flag_correct
                    FROM {PREDICTIONS_TABLE} p
                    JOIN {FLAGS_TABLE} f ON f.issued_at = p.target_issue;
                """)
                cur.executemany(f"""
                    INSERT INTO {PREDICTIONS_TABLE} (base_issue, target_issue, horizon, model_version,
                        pred_level_cd, flag, method, p_black, p_green, p_yellow, p_red)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (base_issue, target_issue, model_version) DO UPDATE
                    SET pred_level_cd = EXCLUDED.pred_level_cd, flag = EXCLUDED.flag, method = EXCLUDED.method,
                        p_black = EXCLUDED.p_black, p_green = EXCLUDED.p_green,
                        p_yellow = EXCLUDED.p_yellow, p_red = EXCLUDED.p_red, created_at = NOW();
                """, [(b, t, h, MODEL_VERSION, lvl, fl, m, p["BLACK"], p["GREEN"], p["YELLOW"], p["RED"])
                      for b, t, h, lvl, p, fl, m in preds])
            conn.commit()
            print(f"\nSaved {len(preds)} predictions to {PREDICTIONS_TABLE}.")


if __name__ == "__main__":
    main()
