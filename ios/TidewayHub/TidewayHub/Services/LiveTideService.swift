import Foundation

/// Richmond tide straight from the source, refreshed every few minutes by the app
/// (the pipeline feed only updates every 6 hours):
///   1. the PLA's own chart data for Richmond (station 14541): predicted, observed and
///      surge every few minutes, plus predicted high/low waters;
///   2. if the PLA refuses the request, the EA Richmond gauge (15-min, ~20 min behind),
///      converted to chart datum.
struct LiveTideService: Sendable {
    static let plaURL = URL(string: "https://pla.co.uk/pla-proxy/one-minute?url=tides/chart/14541")!
    static let eaMeasure = "0009-level-tidal_level-i-15_min-mAOD"

    func load(eaOffset: Double) async -> LiveTide? {
        if let pla = await loadPLA() { return pla }
        return await loadEA(offset: eaOffset)
    }

    // MARK: PLA

    private func loadPLA() async -> LiveTide? {
        let source = "PLA live tide"
        var request = URLRequest(url: Self.plaURL, timeoutInterval: 15)
        request.setValue("application/json, text/plain, */*", forHTTPHeaderField: "Accept")
        request.setValue("https://pla.co.uk/", forHTTPHeaderField: "Referer")
        request.setValue("Mozilla/5.0 (iPhone; CPU iPhone OS 26_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Mobile/15E148 Safari/604.1",
                         forHTTPHeaderField: "User-Agent")
        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            if let http = response as? HTTPURLResponse, http.statusCode != 200 {
                await Diagnostics.shared.record(source, ok: false, summary: "HTTP \(http.statusCode) (falling back to EA gauge)",
                                                detail: snippet(data))
                return nil
            }
            guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                await Diagnostics.shared.record(source, ok: false, summary: "Unexpected response", detail: snippet(data))
                return nil
            }
            let points = (json["heights"] as? [[String: Any]] ?? []).compactMap { row -> TidePoint? in
                guard let time = FlexibleDate.parse(row["tstamp"]) else { return nil }
                return TidePoint(time: time, predicted: number(row["predicted"]),
                                 observed: number(row["observed"]), surge: number(row["surge"]))
            }.sorted { $0.time < $1.time }
            let turns = (json["tpoints"] as? [[String: Any]] ?? []).compactMap { row -> TideTurn? in
                guard let time = FlexibleDate.parse(row["tstamp"]),
                      let state = number(row["tidal_state"]), state == 1 || state == 2 else { return nil }
                return TideTurn(time: time, isHigh: state == 1,
                                level: number(row["predicted"]) ?? number(row["height"]))
            }.sorted { $0.time < $1.time }
            guard !points.isEmpty else {
                await Diagnostics.shared.record(source, ok: false, summary: "No readings in response", detail: snippet(data))
                return nil
            }
            let tide = LiveTide(source: .pla, points: points, turns: turns, fetchedAt: Date())
            await Diagnostics.shared.record(
                source, ok: tide.latestObserved != nil,
                summary: "\(points.count) points, \(turns.count) high/low; latest observed "
                    + (tide.latestObserved.map { "\(UKTime.hm($0.time)) \(String(format: "%.2f", $0.observed ?? 0)) m" } ?? "none"),
                detail: snippet(data, limit: 200))
            return tide
        } catch {
            await Diagnostics.shared.record(source, ok: false, summary: "\(error.localizedDescription) (falling back to EA gauge)")
            return nil
        }
    }

    // MARK: EA fallback

    private func loadEA(offset: Double) async -> LiveTide? {
        let source = "EA Richmond gauge"
        let since = ISO8601DateFormatter().string(from: Date().addingTimeInterval(-36 * 3600))
        var components = URLComponents(string: "https://environment.data.gov.uk/flood-monitoring/id/measures/\(Self.eaMeasure)/readings")!
        components.queryItems = [.init(name: "since", value: since), .init(name: "_sorted", value: ""),
                                 .init(name: "_limit", value: "500")]
        do {
            let (data, _) = try await URLSession.shared.data(for: URLRequest(url: components.url!, timeoutInterval: 30))
            let items = (try JSONSerialization.jsonObject(with: data) as? [String: Any])?["items"] as? [[String: Any]] ?? []
            let points = items.compactMap { row -> TidePoint? in
                guard let time = FlexibleDate.parse(row["dateTime"]), let value = number(row["value"]),
                      value > -2, value < 7 else { return nil }   // drop the gauge's occasional spikes
                return TidePoint(time: time, predicted: nil, observed: value + offset, surge: nil)
            }.sorted { $0.time < $1.time }
            let tide = LiveTide(source: .ea, points: points, turns: [], fetchedAt: Date())
            await Diagnostics.shared.record(
                source, ok: !points.isEmpty,
                summary: "\(points.count) readings (offset \(String(format: "%+.3f", offset)) m); latest "
                    + (tide.latestObserved.map { UKTime.hm($0.time) } ?? "none"),
                detail: snippet(data, limit: 200))
            return points.isEmpty ? nil : tide
        } catch {
            await Diagnostics.shared.record(source, ok: false, summary: error.localizedDescription)
            return nil
        }
    }

    /// Numbers arrive as numbers, strings, single-item lists or null.
    private func number(_ value: Any?) -> Double? {
        switch value {
        case let n as NSNumber: return n.doubleValue
        case let s as String: return Double(s)
        case let list as [Any]: return list.lazy.compactMap { number($0) }.first
        default: return nil
        }
    }
}

struct TidePoint: Identifiable, Sendable {
    let time: Date
    let predicted: Double?
    let observed: Double?
    let surge: Double?
    var id: Date { time }
}

struct TideTurn: Identifiable, Sendable {
    let time: Date
    let isHigh: Bool
    let level: Double?
    var id: Date { time }
}

struct LiveTide: Sendable {
    enum Source: String, Sendable { case pla = "PLA", ea = "EA gauge", feed = "Feed" }

    let source: Source
    let points: [TidePoint]
    let turns: [TideTurn]
    let fetchedAt: Date

    var latestObserved: TidePoint? { points.last { $0.observed != nil } }

    /// "flood" (rising) or "ebb" (falling), from the last 15 minutes of observations.
    var stream: String? {
        guard let latest = latestObserved, let now = latest.observed,
              let earlier = points.last(where: { $0.observed != nil && $0.time <= latest.time.addingTimeInterval(-15 * 60) }),
              let then = earlier.observed else { return nil }
        return now > then ? "flood" : "ebb"
    }

    var upcomingTurns: [TideTurn] { turns.filter { $0.time > Date() } }

    /// The pipeline feed's Richmond series, when live sources are unavailable.
    init(feed richmond: Richmond) {
        source = .feed
        points = richmond.series.map { TidePoint(time: $0.t, predicted: nil, observed: $0.levelCd, surge: nil) }
        turns = []
        fetchedAt = richmond.latestAt
    }

    init(source: Source, points: [TidePoint], turns: [TideTurn], fetchedAt: Date) {
        self.source = source
        self.points = points
        self.turns = turns
        self.fetchedAt = fetchedAt
    }
}

/// Parses the different timestamp styles the PLA and EA use.
enum FlexibleDate {
    private static let withFraction: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f
    }()
    private static let plain = ISO8601DateFormatter()
    private static let noZone: DateFormatter = {
        let f = DateFormatter()
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = TimeZone(identifier: "UTC")
        f.dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
        return f
    }()

    static func parse(_ value: Any?) -> Date? {
        guard var text = value as? String else { return nil }
        if let d = withFraction.date(from: text) ?? plain.date(from: text) { return d }
        text = text.replacingOccurrences(of: " ", with: "T")
        if let d = withFraction.date(from: text) ?? plain.date(from: text) { return d }
        return noZone.date(from: String(text.prefix(19)))
    }
}
