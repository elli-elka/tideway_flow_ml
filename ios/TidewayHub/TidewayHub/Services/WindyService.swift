import CoreLocation
import Foundation

/// Optional: Windy Point Forecast API (https://api.windy.com/point-forecast/docs).
/// Needs your own API key (More > Windy API key). Note Windy's free/testing keys
/// return deliberately shuffled data; a paid key is needed for real values.
struct WindyService: Sendable {
    let apiKey: String
    let model = "iconEu"   // high-resolution European model; "gfs" also works

    func forecast(at coordinate: CLLocationCoordinate2D) async throws -> [WindNow] {
        var request = URLRequest(url: URL(string: "https://api.windy.com/api/point-forecast/v2")!)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONSerialization.data(withJSONObject: [
            "lat": coordinate.latitude, "lon": coordinate.longitude, "model": model,
            "parameters": ["wind", "windGust"], "levels": ["surface"], "key": apiKey,
        ])
        let (data, response) = try await URLSession.shared.data(for: request)
        if let http = response as? HTTPURLResponse, http.statusCode != 200 {
            throw WindyError.http(http.statusCode, String(data: data, encoding: .utf8) ?? "")
        }
        let r = try JSONDecoder().decode(WindyResponse.self, from: data)
        let msToKn = 1.943844
        return r.ts.indices.compactMap { i in
            guard let u = r.windU.at(i), let v = r.windV.at(i) else { return nil }
            let speed = (u * u + v * v).squareRoot() * msToKn
            // u/v give where the wind blows TO; convert to where it comes FROM
            let from = (atan2(-u, -v) * 180 / .pi + 360).truncatingRemainder(dividingBy: 360)
            return WindNow(speedKn: speed, gustKn: r.gust?.at(i).map { $0 * msToKn },
                           fromDegrees: from, time: Date(timeIntervalSince1970: r.ts[i] / 1000))
        }
    }
}

enum WindyError: LocalizedError {
    case http(Int, String)
    var errorDescription: String? {
        switch self {
        case .http(let code, let body): "Windy returned \(code): \(body.prefix(120))"
        }
    }
}

private struct WindyResponse: Decodable {
    let ts: [Double]
    let windU: [Double?]
    let windV: [Double?]
    let gust: [Double?]?

    enum CodingKeys: String, CodingKey {
        case ts
        case windU = "wind_u-surface"
        case windV = "wind_v-surface"
        case gust = "gust-surface"
    }
}
