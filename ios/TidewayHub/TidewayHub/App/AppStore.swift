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
    var observations: [Observation] = []
    var courseWind: [Tideway.Waypoint: WindNow] = [:]
    var windy: [WindNow] = []
    var windyError: String?

    var lastRefresh: Date?
    let location = LocationManager()

    // Settings (edited in More via @AppStorage with the same keys)
    private var feedURLString: String {
        UserDefaults.standard.string(forKey: SettingsKey.feedURL) ?? FeedService.defaultURL.absoluteString
    }
    private var windyAPIKey: String {
        UserDefaults.standard.string(forKey: SettingsKey.windyAPIKey) ?? ""
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

    func refreshFeed() async {
        let url = URL(string: feedURLString) ?? FeedService.defaultURL
        let (feed, source, error) = await FeedService().load(from: url)
        self.feed = feed
        self.feedSource = source
        self.feedError = error
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

    func refreshWindy() async {
        let key = windyAPIKey.trimmingCharacters(in: .whitespaces)
        guard !key.isEmpty else {
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
    static let windyAPIKey = "windyAPIKey"
    static let windUnit = "windUnit"
}
