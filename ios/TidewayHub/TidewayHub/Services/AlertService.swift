import BackgroundTasks
import Foundation
import UserNotifications

/// Local notifications: flag changes at 06:00 and 18:00, sewage discharges near the
/// river, and arriving at your home club.
///
/// There's no server push. iOS wakes the app in the background now and then (it
/// decides how often, usually every 15 minutes to a few hours; more often for apps you
/// open a lot) and the app checks for itself. So alerts can come a little after
/// 06:00 or 18:00, and won't come at all if you've force-quit the app or turned off
/// Background App Refresh.
@MainActor
final class AlertService {
    static let shared = AlertService()
    static let refreshTaskID = "com.example.tidewayhub.app.refresh"

    enum FlagAlerts: String, CaseIterable, Identifiable {
        case off = "Off", upgrades = "Only when it goes up", any = "Any change"
        var id: String { rawValue }
    }

    private let defaults = UserDefaults.standard
    private let center = UNUserNotificationCenter.current()

    var flagAlerts: FlagAlerts {
        FlagAlerts(rawValue: defaults.string(forKey: SettingsKey.flagAlerts) ?? "") ?? .off
    }
    var sewageAlerts: Bool { defaults.bool(forKey: SettingsKey.sewageAlerts) }
    var clubAlerts: Bool { defaults.bool(forKey: SettingsKey.clubAlerts) }

    /// Asks once; returns whether notifications are allowed.
    @discardableResult
    func requestPermission() async -> Bool {
        (try? await center.requestAuthorization(options: [.alert, .sound, .badge])) ?? false
    }

    // MARK: Background refresh

    /// Ask iOS to wake us soon after the next flag issue (or within 30 minutes, for
    /// sewage alerts). iOS treats this as a hint, not a promise.
    func scheduleRefresh() {
        guard flagAlerts != .off || sewageAlerts else { return }
        let request = BGAppRefreshTaskRequest(identifier: Self.refreshTaskID)
        let afterIssue = FlagSchedule.nextIssue().addingTimeInterval(10 * 60)
        request.earliestBeginDate = sewageAlerts ? min(afterIssue, Date().addingTimeInterval(30 * 60)) : afterIssue
        do {
            try BGTaskScheduler.shared.submit(request)
        } catch {
            Diagnostics.shared.record("Background refresh", ok: false, summary: error.localizedDescription)
        }
    }

    /// Runs when iOS wakes the app in the background, and on every foreground refresh.
    func runChecks(offset: Double) async {
        if flagAlerts != .off { await checkFlag(offset: offset) }
        if sewageAlerts, let credentials = StormOverflowService.bundledCredentials,
           let overflows = try? await StormOverflowService(clientID: credentials.id, clientSecret: credentials.secret).nearTideway() {
            await checkSewage(overflows)
        }
        defaults.set(Date(), forKey: SettingsKey.lastBackgroundCheck)
        scheduleRefresh()
    }

    // MARK: Flag

    /// Works out the flag for the latest 06:00 / 18:00 issue from the EA Richmond gauge
    /// (same rule as the PLA) and notifies if it differs from the previous issue.
    func checkFlag(offset: Double) async {
        guard let result = await GaugeFlag.latest(offset: offset) else { return }
        let lastIssue = defaults.object(forKey: SettingsKey.lastFlagIssue) as? Date
        guard lastIssue == nil || result.issue > lastIssue! else { return }
        let previous = defaults.string(forKey: SettingsKey.lastFlag).flatMap(FlagColour.init(rawValue:))
        defaults.set(result.issue, forKey: SettingsKey.lastFlagIssue)
        defaults.set(result.flag.rawValue, forKey: SettingsKey.lastFlag)

        guard let previous, previous != result.flag else { return }
        let order: [FlagColour] = [.black, .green, .yellow, .red]
        let wentUp = (order.firstIndex(of: result.flag) ?? 0) > (order.firstIndex(of: previous) ?? 0)
        if flagAlerts == .upgrades && !wentUp { return }

        let content = UNMutableNotificationContent()
        content.title = "Ebb Tide Flag \(wentUp ? "up" : "down") to \(result.flag.title.uppercased())"
        content.body = "\(previous.title) → \(result.flag.title) at \(UKTime.hm(result.issue)): Richmond low water "
            + String(format: "%.2f m. ", result.level) + "\(result.flag.summary). Worked out from the EA gauge; "
            + "check the official PLA flag before going out."
        content.sound = .default
        content.interruptionLevel = wentUp ? .timeSensitive : .active
        content.threadIdentifier = "flag"
        await post(content, id: "flag-\(result.issue.timeIntervalSince1970)")
    }

    // MARK: Sewage

    func checkSewage(_ overflows: [StormOverflow]) async {
        let discharging = overflows.filter { $0.status == .discharging }
        let known = Set(defaults.stringArray(forKey: SettingsKey.notifiedOverflows) ?? [])
        let fresh = discharging.filter { !known.contains($0.id) }
        // Forget ones that have stopped, so a new discharge from them alerts again
        defaults.set(discharging.map(\.id), forKey: SettingsKey.notifiedOverflows)
        guard !fresh.isEmpty else { return }

        let content = UNMutableNotificationContent()
        if fresh.count == 1, let one = fresh.first {
            content.title = "Sewage discharging near \(one.nearestPlace)"
            content.body = "\(one.name) storm overflow into \(one.watercourse.isEmpty ? "the river" : one.watercourse) "
                + "started \(one.statusSince.map(UKTime.dayHM) ?? "recently"). Avoid swallowing river water; "
                + "cover cuts and wash hands after outings."
        } else {
            content.title = "\(fresh.count) storm overflows discharging near the Tideway"
            content.body = fresh.prefix(4).map { "\($0.name) (\($0.nearestPlace))" }.joined(separator: ", ")
                + (fresh.count > 4 ? " and more." : ".")
        }
        content.sound = .default
        content.threadIdentifier = "sewage"
        await post(content, id: "sewage-\(Date().timeIntervalSince1970)")
    }

    // MARK: Home club

    /// Called when you arrive at your home club: a lock-screen summary for the outing.
    func arrivedAtClub(_ club: Club) async {
        guard clubAlerts else { return }
        // At most one every 3 hours, however often the geofence triggers
        if let last = defaults.object(forKey: SettingsKey.lastClubAlert) as? Date,
           Date().timeIntervalSince(last) < 3 * 3600 { return }
        defaults.set(Date(), forKey: SettingsKey.lastClubAlert)

        let service = FeedService()
        let feed = await service.loadLive(from: FeedService.defaultURL).feed ?? service.loadOffline().feed
        var lines: [String] = []
        if let flag = feed?.currentFlag {
            lines.append("Flag \(flag.flag.title) (gauge, \(UKTime.hm(flag.issuedAt)))")
        }
        if let next = feed?.predictions?.issues.first {
            lines.append("forecast \(UKTime.hm(next.issueAt)): \(next.flag.title)")
        }
        if let tide = feed?.tides?.first(where: { $0.t > Date() }) {
            lines.append("next \(tide.isHigh ? "high" : "low") water \(UKTime.hm(tide.t))")
        }
        let content = UNMutableNotificationContent()
        content.title = "At \(club.name)"
        content.body = (lines.isEmpty ? "Open Tideway Hub for the flag, tide and wind." : lines.joined(separator: " · "))
            + ". Check the official PLA flag before boating."
        content.interruptionLevel = .timeSensitive
        content.threadIdentifier = "club"
        await post(content, id: "club-\(Date().timeIntervalSince1970)")
    }

    private func post(_ content: UNMutableNotificationContent, id: String) async {
        do {
            try await center.add(UNNotificationRequest(identifier: id, content: content, trigger: nil))
            Diagnostics.shared.record("Notifications", ok: true, summary: "Sent: \(content.title)")
        } catch {
            Diagnostics.shared.record("Notifications", ok: false, summary: error.localizedDescription)
        }
    }
}

/// The PLA's rule applied to the EA Richmond gauge: the lowest level (chart datum) in
/// the 12 hours before the latest 06:00 or 18:00 issue.
enum GaugeFlag {
    struct Result {
        let issue: Date
        let flag: FlagColour
        let level: Double
    }

    static func latest(offset: Double, now: Date = .now) async -> Result? {
        let issue = FlagSchedule.previousIssue(before: now)
        let start = issue.addingTimeInterval(-12 * 3600)
        var components = URLComponents(string: "https://environment.data.gov.uk/flood-monitoring/id/measures/\(LiveTideService.eaMeasure)/readings")!
        components.queryItems = [.init(name: "since", value: ISO8601DateFormatter().string(from: start)),
                                 .init(name: "_sorted", value: ""), .init(name: "_limit", value: "200")]
        guard let response = try? await URLSession.shared.data(for: URLRequest(url: components.url!, timeoutInterval: 30)),
              let json = try? JSONSerialization.jsonObject(with: response.0) as? [String: Any],
              let items = json["items"] as? [[String: Any]]
        else { return nil }
        let readings: [(Date, Double)] = items.compactMap { row in
            guard let t = FlexibleDate.parse(row["dateTime"]), t <= issue,
                  let v = (row["value"] as? NSNumber)?.doubleValue, v > -2, v < 7 else { return nil }
            return (t, v)
        }.sorted { $0.0 < $1.0 }
        // The gauge reports up to an hour late: wait until the window is nearly complete
        guard readings.count >= 36, let last = readings.last, last.0 >= issue.addingTimeInterval(-45 * 60) else { return nil }
        // Drop single-reading spikes (more than 1 m away from both neighbours)
        var clean: [Double] = []
        for i in readings.indices {
            let v = readings[i].1
            let before = i > 0 ? readings[i - 1].1 : v, after = i < readings.count - 1 ? readings[i + 1].1 : v
            if abs(v - before) > 1 && abs(v - after) > 1 { continue }
            clean.append(v)
        }
        guard let low = clean.min() else { return nil }
        let level = low + offset
        return Result(issue: issue, flag: FlagColour(level: level), level: level)
    }
}
