---
title: "Tideway Flow ML and Tideway Hub: technical guide"
subtitle: "How the pipeline, model and app are built"
date: "November 2026"
---

# 1. What this project does

The Port of London Authority (PLA) flies an **Ebb Tide Flag** for the Tideway. At
06:00 and 18:00 UK time it looks at the lowest tide level recorded at Richmond in the
previous 12 hours, in metres above chart datum (CD):

| Lowest level (m CD) | Flag | Meaning |
|---|---|---|
| below 0.0 | Black | Very low river flow |
| 0.0 to 1.7 | Green | Normal river flow |
| 1.7 to 2.6 | Yellow | Increased river flow |
| 2.6 or above | Red | High river flow |

A high low-water means a lot of fresh water is coming over Teddington weir, so the ebb
runs hard. This project has two parts:

* **The pipeline** (`pipeline/`, Python): collects the data, rebuilds the flag history,
  forecasts the next 7 days of flags and publishes a small JSON feed.
* **Tideway Hub** (`ios/TidewayHub/`, SwiftUI): an iPhone and iPad app with a widget
  that shows the official flag, our own check of it, the forecast, tides, river flow,
  weather, wind on the course and sewage overflows.

The two never talk directly. The pipeline writes `feed.json` to GitHub Pages and the
app reads it. Live sources that change minute by minute (tide, Kingston flow, sewage,
weather) the app fetches itself.

# 2. Architecture

```
 Sources              GitHub Actions             Postgres         GitHub Pages
 EA flood API  ──┐    ingest_*.py
 EA archive      │    backfill_*.py              readings,
 EA Hydrology    ├──> build_ebb_flags.py  ─────> flags,      ───> feed.json ──> app
 Open-Meteo      │    predict_flags.py           predictions                └─> widget
 NRFA          ──┘    publish_app_feed.py

 The app also reads directly: PLA live tide (else EA Richmond), PLA flag widget,
 EA Kingston, Thames Water overflows, Open-Meteo UKMO weather, METARs, Windy.
```

## Why a database and a static feed

* Postgres holds years of 15-minute readings, which the model needs and an app can't
  download.
* A static JSON file on GitHub Pages is free, fast and has no server to keep running. It
  also keeps the database password out of the app entirely.

# 3. Data sources

| Source | Data | Used by | Key? |
|---|---|---|---|
| EA flood-monitoring API | Richmond tidal level (station 0009, mAOD), Kingston flow (3400TH), Isfield rain gauge (E8290) | pipeline, app | No |
| EA daily archive CSVs | The same readings, back years | `backfill_ea_archive.py` | No |
| EA Hydrology API | Daily flows at 9 upstream gauges, groundwater boreholes | pipeline | No |
| NRFA | Long daily Kingston flow record | `backfill_kingston_nrfa.py` | No |
| Open-Meteo | Catchment rain (ERA5 archive), forecast snapshots, past forecasts, soil moisture, evapotranspiration; UKMO weather in the app | pipeline, app | No |
| PLA | Live tide chart for Richmond (14541); embeddable flag widget | app (PLA blocks GitHub's servers) | No |
| aviationweather.gov | Heathrow and London City METARs | app | No |
| Thames Water Open Data | Live storm overflow status | app | Yes (free) |
| Windy | Point forecast comparison | app, optional | Yes |

Licences and attribution are covered in section 11.

# 4. Database

All tables are created by the scripts themselves (`CREATE TABLE IF NOT EXISTS`), so
a new empty database just works.

| Table | Contents | Written by |
|---|---|---|
| `richmond_tidal_levels` | EA Richmond level, 15-min, mAOD | `ingest_ea_levels.py`, archive backfill |
| `richmond_pla_levels` | PLA observed, predicted and surge, m CD | `etl_pla.py` (run locally) |
| `richmond_ebb_flags` | One row per 06:00/18:00 issue: flag, low level, time of low, source | `build_ebb_flags.py` |
| `kingston_flow_readings` | Kingston flow, 15-min, m³/s | `ingest_kingston_flow.py`, backfill |
| `kingston_daily_flow_nrfa` | Kingston daily mean flow, long record | `backfill_kingston_nrfa.py` |
| `river_flow_daily` | Daily flow per upstream gauge | `ingest_upstream_flows.py` |
| `groundwater_daily` | Daily borehole level per aquifer area | `ingest_groundwater.py` |
| `hydrology_stations` | Station lookups for the two above | hydrology scripts |
| `catchment_rain_daily` | Daily rain, ET₀ and soil moisture at 9 catchment points | `ingest_catchment_rain.py` |
| `catchment_rain_forecast` | Forecast rain per issue date, target day and lead | `ingest_catchment_rain.py`, `backfill_rain_forecasts.py` |
| `isfield_rainfall_readings` | E8290 rain gauge | `ingest_isfield_rain.py` |
| `ebb_flag_predictions` | Every forecast made: target issue, horizon, level, probabilities, method, inputs (JSON) | `predict_flags.py` |
| `ebb_flag_prediction_scores` (view) | Each past prediction joined to the flag that actually happened | `predict_flags.py` |
| `ea_archive_backfill_log` | Which archive days have been tried | `backfill_ea_archive.py` |

Readings tables have `ts TIMESTAMPTZ PRIMARY KEY`, so inserts use
`ON CONFLICT DO NOTHING` and every script can be re-run safely.

# 5. Scheduled jobs

| Workflow | Runs (UTC) | Steps |
|---|---|---|
| `daily_ingest.yml` | 00, 03, 05, 06, 09, 12, 15, 17, 18, 21 at :08 | Richmond levels → Kingston flow → flags → predictions → feed → Pages |
| `kingston_flow_ingest.yml` | every 6 h | Kingston flow |
| `rainfall_ingest.yml` | every 6 h | Isfield rain |
| `catchment_rain_ingest.yml` | daily 05:11 | Rain history, soil, forecast snapshot |
| `hydrology_ingest.yml` | daily 07:29 | Upstream flows, groundwater |
| `backfill_ea_archive.yml` | nightly 02:13 | 45 missing archive days, then `build_ebb_flags.py --full` |
| `pla_scrape_ingest.yml` | every 6 h | PLA data (fails: PLA blocks GitHub) |
| `keepalive.yml` | monthly | Keeps scheduled workflows enabled |

GitHub doesn't guarantee scheduled runs. On quiet repositories it often delays or
skips them, sometimes for hours. That's why the feed job runs often, at extra slots
just after 06:00 and 18:00 UK time (05/17 UTC in summer, 06/18 UTC in winter), and
refreshes Kingston itself rather than relying on the Kingston workflow. It's also
why the app reads time-critical sources directly.

Secrets: `DATABASE_URL` (Actions secret). Optional variable: `RICHMOND_CD_OFFSET`, to
pin the datum offset instead of calibrating it.

# 6. Rebuilding the flag history (`build_ebb_flags.py`)

## Chart datum offset

The EA gauge reports metres above Ordnance Datum (mAOD); the flag uses chart datum. The
difference is constant, but it isn't published, so the script **calibrates it**:

1. Where both PLA and EA readings exist, find each tidal low water in both series.
2. Take the difference (PLA − EA) at each pair of lows. The median of those
   differences is the offset, which currently comes to about +0.705 m from 135 lows.
3. It needs at least 4 pairs (`MIN_CALIBRATION_LOWS`). `RICHMOND_CD_OFFSET` overrides it.

Low waters are used because that's what the flag measures. The offset is also published
in the feed (`richmond_cd_offset`), so the app converts live EA readings the same way.

## Cleaning

* Readings outside a plausible range are dropped: EA −2 to 7 mAOD, PLA −1.5 to
  8 m CD.
* Single-reading spikes more than 1 m from the median of their neighbours are dropped
  (`drop_spikes`).
* A 12-hour window needs at least 36 of its 48 possible readings, otherwise that issue
  is skipped.

## Rule

For each 06:00 and 18:00 (UK time, so it follows the clocks): take the minimum level in
the previous 12 hours. PLA readings are used where available, EA plus the offset
otherwise. Classify with the thresholds above.

# 7. The forecast model (`predict_flags.py`)

## What is predicted

For each of the next 14 issues (12 h to 7 days ahead), the model predicts the
**12-hour low-water level**; the flag follows from the thresholds. The models predict
the *change* from the current low (the "anchor"), not the level itself. That keeps
them honest in long dry or wet spells.

## Features

Each training row is one (issue, horizon) pair, built only from data available at the
issue time:

* **Recent lows:** current, previous, 2-day mean, 7-day min and max, 2-day trend.
* **Tides:** spring–neap phase (sine/cosine of the moon's 14.77-day cycle), at the issue
  and at the target. When the PLA prediction is stored, also its predicted low.
* **Rain:** catchment rain over the last 1, 3, 7, 14 and 30 days, by calendar date.
  ERA5 rain arrives about 6 days late, so the latest days use the forecast issued that
  same day. There's also forecast rain between now and the target (`rain_ahead`) and
  in the 5 days before the target, from the forecast snapshot of the issue date.
* **River and ground:** Kingston and upstream flows and their 3-day change,
  groundwater levels, soil moisture, 30-day evapotranspiration.
* **Calendar:** horizon, morning or evening target.

Features with fewer than two values, or only one distinct value, are dropped before
fitting.

## Methods

| Method | What it is |
|---|---|
| `same_as_now` | Persistence: the next low equals the current one |
| `ridge` | Linear regression with an L2 penalty (impute, scale, α = 10) |
| `gbm` | Histogram gradient-boosted trees (depth 3, 300 rounds, learning rate 0.05) |

## Testing and choosing

A single train/test split was misleading: the last quarter of the data was a summer
drought, where persistence is unbeatable. The script now runs a **walk-forward
backtest**. For each month it trains on everything before that month (at least
60 issues) and predicts that month. The errors are pooled across all months, so wet
and dry spells both count.

For each horizon the method with the lowest mean absolute error is used. The spread of
its errors sets the forecast's uncertainty. The probabilities of each flag come from a
normal distribution around the predicted level with that spread (`flag_probabilities`).

## What to expect

In settled dry weather the forecast looks flat because persistence wins. That's correct,
not a bug. After rain, flows and rain-ahead features move the forecast. Every forecast
is stored with its inputs, and `ebb_flag_prediction_scores` compares it with what
happened, which gives a running track record.

## Tide times (`tides.py`)

The PLA blocks GitHub's servers, so the pipeline can't get PLA tide tables. Instead it
fits a **harmonic tide model** (22 constituents, including shallow-water overtides) to
the last 30 days of EA Richmond readings by least squares, then predicts 7 days ahead.
High-water times are within about 5–15 minutes of the PLA's; low waters are flatter, so
those times are within about 15–35 minutes. These are used only for tide times. Using
them to adjust the flag forecast made it worse.

# 8. The feed (`publish_app_feed.py` → `feed.json`)

```json
{
  "version": 1,
  "generated_at": "2026-11-15T07:08:41+00:00",
  "disclaimer": "...",
  "official_flag_url": "https://pla.co.uk/ebb-tide-flag-warning",
  "current_flag": {"issued_at": "...", "flag": "GREEN", "level_cd": 1.12, "low_at": "...", "source": "ea"},
  "recent_flags": [ ... 14 issues ... ],
  "predictions": {"base_issue": "...", "model_version": "scratch-v2",
                  "issues": [{"issue_at": "...", "horizon": 1, "flag": "GREEN", "level_cd": 1.05,
                              "probabilities": {"BLACK": 0.0, "GREEN": 0.93, "YELLOW": 0.07, "RED": 0.0},
                              "method": "gbm"}]},
  "richmond": {"source": "ea", "latest_at": "...", "level_cd": 2.31, "stream": "flood", "series": [...]},
  "kingston_flow": {"latest_at": "...", "flow_m3s": 41.2, "change_24h": -1.3, "series": [...]},
  "tides": [{"t": "...", "type": "high", "predicted_cd": 4.1}],
  "tides_source": "estimated",
  "rain_forecast": [{"day": "2026-11-15", "catchment_mm": 3.2, "chance": 70}],
  "richmond_cd_offset": 0.705
}
```

* Times are UTC ISO 8601. The app shows everything in UK time on a 24-hour clock.
* Each section is optional, and the app decodes them independently, so one bad section
  can't hide the rest.
* `FEED_NOW=...` makes the script pretend it's another time, for testing on old data.

# 9. The app (`ios/TidewayHub`)

SwiftUI for iOS 26 with Liquid Glass. The Xcode project is generated from `project.yml`
by XcodeGen, so the `.xcodeproj` is never committed.

## Structure

```
TidewayHub/
  App/        TidewayHubApp (entry, background task), RootView (tabs), AppStore (state), Diagnostics
  Models/     Feed (feed.json types), FlagColour, Time (UK time and flag schedule helpers)
  Domain/     Tideway (course waypoints, crew wind), SunTimes, Clubs
  Services/   FeedService, LiveTideService, KingstonService, StormOverflowService,
              WeatherService, ObservationService, WindyService, LocationManager, AlertService
  Views/      FlagView, RiverView, TideChartCard, WeatherView, WindView, WaterView, SettingsView
  Views/Components/  Glass cards, river background, flag gauge, PLA widget, formatting
TidewayWidget/       Home Screen and Lock Screen widget (shares Models and FeedService)
```

`AppStore` is a single `@Observable` object holding everything the screens show. Each
`refresh…()` method loads one source. They run in parallel on launch and when the app
comes to the foreground, then on a timer while it's open: tide every 5 minutes; weather,
Kingston and sewage every 15; the feed every 30.

## Tabs

| Tab | Shows |
|---|---|
| Flag | Official PLA flag (large); our gauge check and whether it matches; the experimental forecast with probabilities and upstream rain; "How this works" |
| River | Scrollable tide chart (observed, predicted, surge, high/low waters, readout); high and low water for 3 days; Kingston flow; daylight |
| Weather | Now (including measured airport wind); wind on the course (map); next 24 hours; 7 days |
| Water | Storm overflows near the river: discharging now, last 48 h, map, advice |
| More | Units, alerts, home club, flag legend, data sources, diagnostics |

## How each flag is shown

1. **Official.** `PLAFlagWidget` loads the PLA's embeddable widget in a hidden web view
   and reads its text, class names and images to find which flag it shows. If that
   works, the app draws it natively. If not, it shows the widget itself at its published
   382×442 size.
2. **Our check.** The pipeline's latest issue, from the EA gauge using the PLA rule.
3. **Forecast.** Dashed chips and an "experimental" badge; days beyond 3 are faded.

## Live sources in the app

* **Tide:** the PLA's live chart data, else the EA Richmond gauge plus the offset, else
  the feed's series.
* **Tide times:** the PLA's live times (about one day), then the feed's times for the
  following days (the PLA's, or the harmonic estimate).
* **Kingston:** the EA gauge directly, falling back to the feed; whichever is newer.
* **Sewage:** Thames Water's discharge status for every overflow. Grid references are
  converted to GPS (OSGB36 to WGS84). Overflows within 1.5 km of the river from Walton
  to Putney are kept, or within 6 km if they discharge into a tributary.

## Alerts and the widget

* **Flag changes.** iOS wakes the app in the background now and then
  (`BGAppRefreshTask`). The app works out the latest 06:00 or 18:00 flag from the EA
  gauge and posts a local notification if it changed: any change, or only when it goes
  up. iOS decides the timing, so alerts can be a little late.
* **Sewage.** The same background check posts an alert when an overflow near the river
  starts discharging.
* **Home club.** A 300 m geofence round your club, run by iOS even when the app is
  closed. On arrival it posts a lock-screen notification with the flag, forecast and
  next tide. It needs "Always" location permission.
* **Widget.** Small, medium and Lock Screen sizes. It reads `feed.json` itself and
  refreshes about every 30 minutes and after each issue.

iOS doesn't let an app put a widget on the Lock Screen by itself, so the notification on
arrival is the closest equivalent. Real push notifications at exactly 06:00 and 18:00
would need a paid developer account and a server holding Apple push credentials;
section 12 explains how.

## Conventions that matter

* JSON field names are mapped with explicit `CodingKeys`. `convertFromSnakeCase` turned
  `temperature_2m` into `temperature2M` and broke decoding.
* All times go through `UKTime` (24-hour, Europe/London).
* Don't name types after Apple modules (`Observation` clashed with the Observation
  framework).
* Charts keep every series in the chart and hide them with opacity. Adding and removing
  series marks broke the scrolling chart.
* Every source records what it last returned in `Diagnostics` (More > Advanced). That's
  the first place to look when something shows "no data".

## Keys

Optional keys go in `Config/Secrets.xcconfig` (git-ignored). Copy
`Secrets.example.xcconfig` to make it.

* `WINDY_API_KEY`
* `THAMES_WATER_CLIENT_ID`
* `THAMES_WATER_CLIENT_SECRET`

They're compiled into `Info.plist`. Keys in a shipped app can be extracted, so before
the App Store these should move behind a small server or the pipeline.

# 10. Common changes

| To… | Change |
|---|---|
| Add a data source to the model | Ingest script plus table, a workflow step, then load it in `predict_flags.load()` and add features in `daily_features()` |
| Change flag thresholds | `THRESHOLDS` in `predict_flags.py`, `build_ebb_flags.py`, and `FlagColour.init(level:)` in the app |
| Add a field to the app | Add it in `publish_app_feed.py`, then to `Feed.swift` with a `CodingKey` |
| Add a screen | New view in `Views/`, a `Tab` in `RootView`, re-run `xcodegen` |
| Add a club | `Club.tideway` in `Domain/Clubs.swift` |
| Test the pipeline locally | Local Postgres, load CSV exports, set `DATABASE_URL`, run the scripts; `FEED_NOW` for old data |

# 11. Licences and attribution

| Source | Terms (check before publishing) | Attribution |
|---|---|---|
| Environment Agency | Open Government Licence v3; commercial use allowed | "Contains Environment Agency data licensed under the Open Government Licence v3.0" |
| Open-Meteo | Free API is for non-commercial use; commercial use needs a subscription. Data CC BY 4.0 | "Weather data by Open-Meteo.com" plus the model's source (Met Office) |
| NRFA | Check NRFA's data terms | "National River Flow Archive" |
| PLA | No public data licence found; the flag widget is offered for embedding, but other PLA data isn't. Ask the PLA | Don't use the PLA logo or imply it's official |
| Thames Water | Terms accepted when registering | As required by those terms |
| aviationweather.gov | US government data | Credit as the source |
| Windy | Paid plans for production use | Windy's logo and attribution rules |

# 12. Towards the App Store

1. Apple Developer Program (£79/year) for TestFlight and the App Store.
2. Move API keys out of the app (pipeline or a small proxy).
3. Licences: an Open-Meteo commercial plan if the app earns money; written permission
   from the PLA for the flag widget and tide data.
4. Push notifications at exactly 06:00 and 18:00: have the pipeline compute the flag
   and send it through Apple Push Notification service. That needs an APNs key and
   somewhere to store device tokens, for example a small table written by a serverless
   function.
5. A privacy policy and App Store privacy labels (location; no tracking).
6. Safety wording in the listing: unofficial, always check the PLA.
