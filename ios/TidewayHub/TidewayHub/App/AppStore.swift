import CoreLocation
import Observation
import SwiftUI

/// App-wide state: the pipeline feed plus live weather and wind.
@MainActor @Observable
final class AppStore {
    // Pipeline feed (flags, predictions, levels)
    var feed: Feed?
    var feedSource: FeedService.Source = .sample
    var feedError: String?

    // Weather and wind
    var weather: WeatherForecast?
    var weatherError: String?
    var observations: [WindObservation] = []
    var courseWind: [Tideway.Waypoint: WindNow] = [:]
    var windy: [WindNow] = []
    var windyError: String?

    var lastRefresh: Date?
    let location = LocationManager()

    // Developer overrides (not shown in the UI)
    private var feedURLString: String {
        UserDefaults.standard.string(forKey: SettingsKey.feedURL) ?? FeedService.defaultURL.absoluteString
    }

    /// Your position if shared, otherwise Putney.
    var coordinate: CLLocationCoordinate2D {
        location.location?.coordinate ?? Tideway.defaultCoordinate
    }

    var reach: Tideway.Reach { Tideway.nearestReach(to: coordinate) }

    /// Wind to use for crew calculations: Windy if configured, else the UKMO forecast.
    var windHere: WindNow? {
        windy.min(by: { abs($0.time.timeIntervalSinceNow) < abs($1.time.timeIntervalSinceNow) })
            ?? weather?.current?.wind
    }

    func refreshAll() async {
        location.request()
        await withTaskGroup(of: Void.self) { group in
            group.addTask { await self.refreshFeed() }
            group.addTask { await self.refreshWeather() }
            group.addTask { await self.refreshObservations() }
            group.addTask { await self.refreshCourseWind() }
            group.addTask { await self.refreshWindy() }
        }
        lastRefresh = Date()
    }

    init() {
        // Show something immediately; the live feed replaces it when it arrives
        let offline = FeedService().loadOffline()
        feed = offline.feed
        feedSource = offline.source
        feedError = offline.error
    }

    func refreshFeed() async {
        let url = URL(string: feedURLString) ?? FeedService.defaultURL
        let live = await FeedService().loadLive(from: url)
        if let fresh = live.feed {
            feed = fresh
            feedSource = .live
            feedError = nil
        } else {
            feedError = live.error
            if feed == nil {
                let offline = FeedService().loadOffline()
                feed = offline.feed
                feedSource = offline.source
                feedError = [live.error, offline.error].compactMap { $0 }.joined(separator: " ")
            }
        }
    }

    func refreshWeather() async {
        do {
            weather = try await WeatherService().forecast(at: coordinate)
            weatherError = nil
        } catch {
            weatherError = "Forecast unavailable: \(error.localizedDescription)"
        }
    }

    func refreshObservations() async {
        observations = (try? await ObservationService().latest()) ?? observations
    }

    func refreshCourseWind() async {
        let stations = Tideway.stations
        guard let winds = try? await WeatherService().currentWind(at: stations.map(\.coordinate)),
              winds.count == stations.count else { return }
        courseWind = Dictionary(uniqueKeysWithValues: zip(stations, winds))
    }

    /// True when a Windy key was built into the app (see ios/README.md).
    var hasWindy: Bool { WindyService.bundledKey != nil }

    func refreshWindy() async {
        guard let key = WindyService.bundledKey else {
            windy = []
            windyError = nil
            return
        }
        do {
            windy = try await WindyService(apiKey: key).forecast(at: coordinate)
            windyError = nil
        } catch {
            windyError = error.localizedDescription
        }
    }
}

enum SettingsKey {
    static let feedURL = "feedURL"
    static let windUnit = "windUnit"
}
