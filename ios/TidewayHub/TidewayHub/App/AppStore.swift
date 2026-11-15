import CoreLocation
import Observation
import SwiftUI
import WidgetKit

/// App-wide state: the pipeline feed plus live weather and wind.
@MainActor @Observable
final class AppStore {
    // Pipeline feed (flags, predictions, levels)
    var feed: Feed?
    var feedSource: FeedService.Source = .sample
    var feedError: String?

    // The official flag, as read from the PLA's widget (see PLAFlagWidget)
    var officialFlag: FlagColour?
    var officialFlagReadAt: Date?

    /// The flag to colour screens by: official when known, else our gauge reading.
    var displayFlag: FlagColour? { officialFlag ?? feed?.currentFlag?.flag }

    // Live Richmond tide (PLA, else EA), refreshed every few minutes
    var liveTide: LiveTide?

    /// Live tide if available, else the pipeline feed's Richmond series.
    var tide: LiveTide? {
        liveTide ?? feed?.richmond.map { LiveTide(feed: $0) }
    }

    // Kingston flow straight from the EA (the feed's copy can be hours old)
    var liveKingston: KingstonFlow?
    var kingston: KingstonFlow? {
        guard let live = liveKingston else { return feed?.kingstonFlow }
        guard let fromFeed = feed?.kingstonFlow else { return live }
        return live.latestAt >= fromFeed.latestAt ? live : fromFeed
    }

    // Storm overflows (sewage) near the river, from Thames Water
    var overflows: [StormOverflow] = []
    var overflowsError: String?
    var overflowsUpdated: Date?
    var hasThamesWaterKey: Bool { StormOverflowService.bundledCredentials != nil }

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

    /// Your position if shared and near the Tideway, otherwise Putney. (The Simulator
    /// defaults to California, which gave Californian weather with London times.)
    var coordinate: CLLocationCoordinate2D {
        guard let here = location.location, isNearTideway(here) else { return Tideway.defaultCoordinate }
        return here.coordinate
    }

    /// True when we're using the device's real position rather than Putney.
    var usingDeviceLocation: Bool {
        guard let here = location.location else { return false }
        return isNearTideway(here)
    }

    private func isNearTideway(_ location: CLLocation) -> Bool {
        let putney = CLLocation(latitude: Tideway.defaultCoordinate.latitude,
                                longitude: Tideway.defaultCoordinate.longitude)
        return location.distance(from: putney) < 40_000
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
            group.addTask { await self.refreshLiveTide() }
            group.addTask { await self.refreshWeather() }
            group.addTask { await self.refreshObservations() }
            group.addTask { await self.refreshCourseWind() }
            group.addTask { await self.refreshWindy() }
            group.addTask { await self.refreshKingston() }
            group.addTask { await self.refreshOverflows() }
        }
        lastRefresh = Date()
        // Same checks the background refresh does, so alerts also come while the app is open
        await AlertService.shared.runChecks(offset: feed?.richmondCdOffset ?? 0.705)
    }

    func refreshKingston() async {
        if let fresh = await KingstonService().load() { liveKingston = fresh }
    }

    func refreshOverflows() async {
        guard let credentials = StormOverflowService.bundledCredentials else {
            overflowsError = nil
            return
        }
        do {
            overflows = try await StormOverflowService(clientID: credentials.id, clientSecret: credentials.secret).nearTideway()
            overflowsUpdated = Date()
            overflowsError = nil
        } catch {
            overflowsError = error.localizedDescription
        }
    }

    init() {
        // Show something immediately; the live feed replaces it when it arrives
        let offline = FeedService().loadOffline()
        feed = offline.feed
        feedSource = offline.source
        feedError = offline.error
    }

    /// Keeps live data fresh while the app is open: tide every 5 minutes, weather and
    /// wind every 15, the feed every 30. Ends automatically when the view goes away.
    func autoRefresh() async {
        var tick = 0
        while !Task.isCancelled {
            try? await Task.sleep(for: .seconds(300))
            if Task.isCancelled { break }
            tick += 1
            await refreshLiveTide()
            if tick % 3 == 0 {
                await refreshKingston()
                await refreshOverflows()
                await refreshWeather()
                await refreshObservations()
                await refreshCourseWind()
            }
            if tick % 6 == 0 { await refreshFeed() }
        }
    }

    func refreshLiveTide() async {
        // Chart datum offset for the EA gauge, calibrated by the pipeline
        let offset = feed?.richmondCdOffset ?? 0.705
        if let fresh = await LiveTideService().load(eaOffset: offset) {
            liveTide = fresh
        }
    }

    func refreshFeed() async {
        let url = URL(string: feedURLString) ?? FeedService.defaultURL
        let live = await FeedService().loadLive(from: url)
        if let fresh = live.feed {
            feed = fresh
            feedSource = .live
            feedError = nil
            WidgetCenter.shared.reloadAllTimelines()
            Diagnostics.shared.record(
                "Feed (GitHub Pages)", ok: true,
                summary: "Live, generated \(fresh.generatedAt.ago); flag \(fresh.currentFlag?.flag.title ?? "none"), "
                    + "\(fresh.predictions?.issues.count ?? 0) predictions, \(fresh.rainForecast?.count ?? 0) rain days, "
                    + "Kingston \(fresh.kingstonFlow == nil ? "missing" : "ok")")
        } else {
            Diagnostics.shared.record("Feed (GitHub Pages)", ok: false, summary: live.error ?? "Unknown error",
                                      detail: url.absoluteString)
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
        courseWind = Dictionary(uniqueKeysWithValues: zip(stations, winds).compactMap { station, wind in
            wind.map { (station, $0) }
        })
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
    static let showDiagnostics = "showDiagnostics"
    // Notifications and home club
    static let flagAlerts = "flagAlerts"
    static let sewageAlerts = "sewageAlerts"
    static let clubAlerts = "clubAlerts"
    static let homeClub = "homeClub"
    static let homeClubLatitude = "homeClubLatitude"
    static let homeClubLongitude = "homeClubLongitude"
    static let lastFlagIssue = "lastFlagIssue"
    static let lastFlag = "lastFlag"
    static let notifiedOverflows = "notifiedOverflows"
    static let lastClubAlert = "lastClubAlert"
    static let lastBackgroundCheck = "lastBackgroundCheck"
}
