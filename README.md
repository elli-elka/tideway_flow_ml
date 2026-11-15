# Tideway Flow ML

Predicting the Port of London Authority's **Ebb Tide Flag** on the Thames Tideway
(Putney to Richmond), plus **Tideway Hub**, an iPhone/iPad app for coxes, steers and
coaches.

> Unofficial. Always check the [official PLA flag](https://pla.co.uk/ebb-tide-flag-warning)
> before boating.

## How it works, in one picture

```
 Environment Agency  ─┐                                   ┌─> feed.json on GitHub Pages ─> app + widget
 Open-Meteo rain     ─┤   GitHub Actions    Postgres      │
 EA Hydrology / NRFA ─┼─> (pipeline/) ───> (history) ─────┤
 PLA tide data       ─┘   every 3 hours                   └─> prediction scores
                                                              
 The app also reads some sources directly: PLA live tide and flag widget, EA Richmond
 and Kingston gauges, Thames Water storm overflows, Open-Meteo weather, airport wind.
```

1. **Ingest.** Scheduled workflows pull Richmond tide levels, Kingston flow, upstream
   river flows, groundwater, and measured and forecast catchment rain into Postgres.
2. **Flags.** The PLA's rule (the lowest Richmond level in the 12 hours before 06:00 and
   18:00, in metres above chart datum) is applied to the history to rebuild every past
   flag.
3. **Model.** Each run retrains three methods ("same as now", a linear model and a
   gradient-boosted tree model) to predict the next 14 flags (7 days). It keeps
   whichever method scored best in a month-by-month backtest.
4. **Feed.** A small public `feed.json` is published to GitHub Pages for the app.
5. **App.** It shows the official flag, our gauge check, the forecast, tides, Kingston
   flow, weather, wind on the course and sewage overflows. It also has widgets and
   alerts.

## Repository layout

```
pipeline/            Python: ingest, flag history, model, feed     (see docs/technical-guide.md)
ios/TidewayHub/      SwiftUI app and widget                          (see ios/README.md)
.github/workflows/   scheduled jobs that run the pipeline
docs/                technical guide (Markdown and PDF)
```

## Quick start

**Pipeline:**

```sh
cd pipeline
pip install -r requirements.txt -r requirements-ml.txt
echo 'DATABASE_URL=postgresql://...' > ../.env      # git-ignored
python predict_flags.py
```

In GitHub, add a `DATABASE_URL` Actions secret, then enable Pages under
*Settings > Pages > Source: GitHub Actions*.

**App:**

```sh
brew install xcodegen
cd ios/TidewayHub && xcodegen && open TidewayHub.xcodeproj
```

Optional API keys go in `ios/TidewayHub/Config/Secrets.xcconfig`; see `ios/README.md`.

## More detail

[docs/technical-guide.md](docs/technical-guide.md) (also as a PDF) explains how
every part is built: data sources, database tables, the flag rule and datum offset,
the model and how it's tested, the feed format, the app's structure, notifications
and widgets, and how to change things.
