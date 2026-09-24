# Tideway Flow ML

Predicting the PLA Ebb Tide Flag on the Thames Tideway (Putney to Richmond), plus an
iPhone/iPad app for coxes, steers and coaches.

```
pipeline/            Python: data ingest, flag history, prediction model, app feed
ios/TidewayHub/      SwiftUI app (see ios/README.md)
.github/workflows/   scheduled jobs that run the pipeline (from pipeline/)
```

## Pipeline

| Script | What it does | Schedule |
|---|---|---|
| `ingest_ea_levels.py` | Richmond tidal levels (EA) | every 6 h |
| `build_ebb_flags.py` | Reconstructs 06:00/18:00 flags from Richmond lows | after levels |
| `predict_flags.py` | Predicts the next 14 flags, saves to `ebb_flag_predictions` | after flags |
| `publish_app_feed.py` | Writes `feed.json` for the app, deployed to GitHub Pages | after predictions |
| `etl_pla.py` | PLA observed/predicted tide (blocked from GitHub; run locally) | every 6 h |
| `ingest_kingston_flow.py` | Thames flow at Kingston (EA, 15-min) | every 6 h |
| `ingest_upstream_flows.py`, `ingest_groundwater.py` | Daily flows and boreholes (EA Hydrology API) | daily |
| `ingest_catchment_rain.py` | Catchment rain, evapotranspiration, soil moisture + forecast snapshots (Open-Meteo) | daily |
| `ingest_isfield_rain.py` | Local rain gauge E8290 | every 6 h |
| `backfill_ea_archive.py` | Years of Richmond levels, Kingston flow and E8290 rain from the EA daily archive, a batch per night, then rebuilds all flags | nightly until done |
| `backfill_rain_forecasts.py`, `backfill_kingston_nrfa.py` | One-off history backfills | manual |

Run locally: `cd pipeline && pip install -r requirements.txt -r requirements-ml.txt`,
put `DATABASE_URL=...` in a `.env` at the repo root, then e.g. `python predict_flags.py`.

The app feed is served from GitHub Pages: enable it once under
Settings > Pages > Source: **GitHub Actions**.
