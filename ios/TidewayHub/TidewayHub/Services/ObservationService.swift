import Foundation

/// Measured (not forecast) wind from airport weather reports (METARs) via
/// aviationweather.gov, free and no key. Heathrow and London City sit either side of
/// the Tideway.
struct ObservationService: Sendable {
    static let stations = ["EGLL": "Heathrow", "EGLC": "London City"]

    func latest() async throws -> [Observation] {
        let ids = Self.stations.keys.sorted().joined(separator: ",")
        let url = URL(string: "https://aviationweather.gov/api/data/metar?ids=\(ids)&format=json")!
        let (data, _) = try await URLSession.shared.data(from: url)
        let reports = try JSONDecoder().decode([Metar].self, from: data)
        // Newest report per station
        var latest: [String: Metar] = [:]
        for report in reports where (latest[report.icaoId]?.obsTime ?? 0) < report.obsTime {
            latest[report.icaoId] = report
        }
        return latest.values
            .map { m in
                Observation(station: Self.stations[m.icaoId] ?? m.icaoId,
                            time: Date(timeIntervalSince1970: TimeInterval(m.obsTime)),
                            speedKn: m.wspd ?? 0, gustKn: m.wgst,
                            fromDegrees: m.wdir, temperature: m.temp)
            }
            .sorted { $0.station < $1.station }
    }
}

struct Observation: Sendable, Identifiable {
    let station: String
    let time: Date
    let speedKn: Double
    let gustKn: Double?
    /// nil when the wind is variable.
    let fromDegrees: Double?
    let temperature: Double?
    var id: String { station }
}

private struct Metar: Decodable {
    let icaoId: String
    let obsTime: Int
    let wdir: Double?
    let wspd: Double?
    let wgst: Double?
    let temp: Double?

    enum CodingKeys: String, CodingKey { case icaoId, obsTime, wdir, wspd, wgst, temp }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        icaoId = try c.decode(String.self, forKey: .icaoId)
        obsTime = try c.decode(Int.self, forKey: .obsTime)
        wdir = try? c.decode(Double.self, forKey: .wdir)   // "VRB" when variable
        wspd = try? c.decode(Double.self, forKey: .wspd)
        wgst = try? c.decode(Double.self, forKey: .wgst)
        temp = try? c.decode(Double.self, forKey: .temp)
    }
}
