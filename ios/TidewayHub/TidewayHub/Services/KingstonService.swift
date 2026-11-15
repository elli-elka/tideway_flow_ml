import Foundation

/// Thames flow at Kingston (EA station 3400TH, just above Teddington weir), straight
/// from the EA every time the app refreshes, so it doesn't depend on the pipeline's
/// schedule. Falls back to the feed's copy when the EA can't be reached.
struct KingstonService: Sendable {
    /// The station's live 15-minute flow measure (it has had two names over the years).
    static let measures = ["3400TH-flow-water-i-15_min-m3_s", "3400TH-flow--i-15_min-m3_s"]

    func load() async -> KingstonFlow? {
        for measure in Self.measures {
            if let flow = await load(measure: measure) { return flow }
        }
        return nil
    }

    private func load(measure: String) async -> KingstonFlow? {
        let source = "EA Kingston flow"
        let since = ISO8601DateFormatter().string(from: Date().addingTimeInterval(-7 * 86400))
        var components = URLComponents(string: "https://environment.data.gov.uk/flood-monitoring/id/measures/\(measure)/readings")!
        components.queryItems = [.init(name: "since", value: since), .init(name: "_sorted", value: ""),
                                 .init(name: "_limit", value: "1000")]
        do {
            let (data, _) = try await URLSession.shared.data(for: URLRequest(url: components.url!, timeoutInterval: 30))
            let items = (try JSONSerialization.jsonObject(with: data) as? [String: Any])?["items"] as? [[String: Any]] ?? []
            let readings: [(Date, Double)] = items.compactMap { row in
                guard let time = FlexibleDate.parse(row["dateTime"]),
                      let value = (row["value"] as? NSNumber)?.doubleValue, value >= 0 else { return nil }
                return (time, value)
            }.sorted { $0.0 < $1.0 }
            guard let latest = readings.last else {
                await Diagnostics.shared.record(source, ok: false, summary: "No readings for \(measure)", detail: snippet(data))
                return nil
            }
            let dayAgo = readings.last { $0.0 <= latest.0.addingTimeInterval(-86400) }
            // One point every 3 hours is plenty for the sparkline
            var series: [FlowPoint] = []
            for (time, value) in readings where series.last.map({ time.timeIntervalSince($0.t) >= 3 * 3600 }) ?? true {
                series.append(FlowPoint(t: time, flowM3s: value))
            }
            if series.last?.t != latest.0 { series.append(FlowPoint(t: latest.0, flowM3s: latest.1)) }
            await Diagnostics.shared.record(source, ok: true,
                                      summary: "\(readings.count) readings; latest \(UKTime.dayHM(latest.0)) \(String(format: "%.1f", latest.1)) m³/s")
            return KingstonFlow(latestAt: latest.0, flowM3s: latest.1,
                                change24h: dayAgo.map { latest.1 - $0.1 }, series: series)
        } catch {
            await Diagnostics.shared.record(source, ok: false, summary: error.localizedDescription)
            return nil
        }
    }
}
