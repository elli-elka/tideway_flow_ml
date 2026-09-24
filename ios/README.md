# Tideway Hub (iPhone + iPad)

SwiftUI app for coxes, steers and coaches on the Tideway: the current PLA Ebb Tide
Flag, predicted flags for the next week, Richmond levels, weather (Met Office UKMO
model) and wind along the course relative to where you are. Built for iOS 26 with
Liquid Glass.

## Run it in Xcode

Needs Xcode 26 or later on your Mac. No paid Apple Developer account needed for the
Simulator or your own iPhone.

```sh
brew install xcodegen            # once
cd ios/TidewayHub
xcodegen                         # generates TidewayHub.xcodeproj from project.yml
open TidewayHub.xcodeproj
```

Pick an iPhone or iPad simulator and press Run. To run on your own phone: select
the TidewayHub target > Signing & Capabilities > Team = your Apple ID, and change the
bundle identifier (`com.example.tidewayhub.app`) to something unique. Free-account
installs expire after 7 days; just run again from Xcode.

Re-run `xcodegen` whenever you add or remove Swift files.

## Where the data comes from

| Screen | Source | Key needed |
|---|---|---|
| Flag, predictions, Richmond, Kingston, tides | `feed.json` published by `pipeline/publish_app_feed.py` to GitHub Pages | No |
| Weather | Open-Meteo, Met Office UKMO model (CC BY 4.0) | No |
| Measured wind | Heathrow / London City METARs via aviationweather.gov | No |
| Official current flag | PLA's embeddable Ebb Tide Flag widget | No |
| Windy comparison (optional) | Windy Point Forecast API | Yes, built in (below) |

### Optional Windy key

Copy `Config/Secrets.example.xcconfig` to `Config/Secrets.xcconfig` (git-ignored), put
your key in it, and re-run `xcodegen`. The key is compiled into the app, so users
never see it; without a key the Windy card is hidden. Note that keys shipped inside
an app can be extracted, and Windy's free testing keys return shuffled data.

Until the feed is live (or when offline) the app shows the bundled
`Resources/SampleFeed.json` (real Richmond data from July 2026) with a banner saying so.

## Layout

```
TidewayHub/
  App/        entry point, tabs, AppStore (all app state and refreshing)
  Models/     feed.json types, flag colours
  Services/   feed, weather, observations, Windy, location
  Domain/     Tideway course waypoints, crew-relative wind, wind-against-tide
  Views/      Flag, Weather, Wind, More + shared Liquid Glass components
  Resources/  asset catalog, SampleFeed.json
```

This app is unofficial: always check the official PLA flag before boating.
