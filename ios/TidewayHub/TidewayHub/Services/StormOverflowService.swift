import CoreLocation
import Foundation

/// Live storm overflow (sewage discharge) status from Thames Water's Open Data API:
/// every storm overflow's monitor (EDM) says whether it is discharging right now.
/// We keep the ones that discharge into or near the Thames between Walton and Putney.
///
/// Needs a free Thames Water Open Data account (data.thameswater.co.uk): put its
/// client ID and secret in Config/Secrets.xcconfig (see ios/README.md).
struct StormOverflowService: Sendable {
    static let endpoint = URL(string: "https://prod-tw-opendata-app.uk-e1.cloudhub.io/data/STE/v1/DischargeCurrentStatus")!

    static var bundledCredentials: (id: String, secret: String)? {
        let id = Bundle.main.object(forInfoDictionaryKey: "ThamesWaterClientID") as? String ?? ""
        let secret = Bundle.main.object(forInfoDictionaryKey: "ThamesWaterClientSecret") as? String ?? ""
        let trimmedID = id.trimmingCharacters(in: .whitespaces), trimmedSecret = secret.trimmingCharacters(in: .whitespaces)
        guard !trimmedID.isEmpty, !trimmedSecret.isEmpty, !trimmedID.hasPrefix("$(") else { return nil }
        return (trimmedID, trimmedSecret)
    }

    let clientID: String
    let clientSecret: String

    enum Failure: LocalizedError {
        case http(Int), unreadable
        var errorDescription: String? {
            switch self {
            case .http(401), .http(403): "Thames Water refused the API key (HTTP 401/403). Check the client ID and secret."
            case .http(let code): "Thames Water returned HTTP \(code)."
            case .unreadable: "Thames Water's response couldn't be read."
            }
        }
    }

    /// All overflows near the Tideway, discharging ones first.
    func nearTideway() async throws -> [StormOverflow] {
        var all: [StormOverflow] = []
        var offset = 0
        let pageSize = 1000
        // A few pages at most (Thames Water has a few hundred overflows)
        for _ in 0..<5 {
            let page = try await fetch(offset: offset, limit: pageSize)
            all += page.overflows
            if page.rows < pageSize { break }
            offset += pageSize
        }
        let near = all.compactMap { $0.locatedNearTideway() }
        await Diagnostics.shared.record("Thames Water overflows", ok: true,
                                        summary: "\(all.count) overflows, \(near.count) near the Tideway, "
                                            + "\(near.filter { $0.status == .discharging }.count) discharging")
        return near.sorted { ($0.status.rank, $0.distanceToRiver) < ($1.status.rank, $1.distanceToRiver) }
    }

    private func fetch(offset: Int, limit: Int) async throws -> (overflows: [StormOverflow], rows: Int) {
        var components = URLComponents(url: Self.endpoint, resolvingAgainstBaseURL: false)!
        components.queryItems = [.init(name: "limit", value: "\(limit)"), .init(name: "offset", value: "\(offset)")]
        var request = URLRequest(url: components.url!, timeoutInterval: 30)
        request.setValue(clientID, forHTTPHeaderField: "client_id")
        request.setValue(clientSecret, forHTTPHeaderField: "client_secret")
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        let (data, response) = try await URLSession.shared.data(for: request)
        if let http = response as? HTTPURLResponse, http.statusCode != 200 {
            await Diagnostics.shared.record("Thames Water overflows", ok: false, summary: "HTTP \(http.statusCode)",
                                            detail: snippet(data))
            throw Failure.http(http.statusCode)
        }
        guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let items = json["items"] as? [[String: Any]] else {
            await Diagnostics.shared.record("Thames Water overflows", ok: false, summary: "Unexpected response",
                                            detail: snippet(data))
            throw Failure.unreadable
        }
        return (items.compactMap(StormOverflow.init(row:)), items.count)
    }
}

struct StormOverflow: Identifiable, Sendable, Hashable {
    enum Status: String, Sendable {
        case discharging = "Discharging", notDischarging = "Not discharging", offline = "Offline", unknown = "Unknown"
        var rank: Int {
            switch self {
            case .discharging: 0
            case .notDischarging: 1
            case .offline: 2
            case .unknown: 3
            }
        }
    }

    let id: String
    let name: String
    let watercourse: String
    let status: Status
    /// When the status last changed.
    let statusSince: Date?
    let lastStart: Date?
    let lastStop: Date?
    let dischargedInPast48h: Bool
    let latitude: Double
    let longitude: Double
    var distanceToRiver: Double = .infinity
    var nearestPlace: String = ""

    var coordinate: CLLocationCoordinate2D { .init(latitude: latitude, longitude: longitude) }

    /// Discharging now, or stopped within the last 48 hours.
    var isRecent: Bool { status == .discharging || dischargedInPast48h }

    init?(row: [String: Any]) {
        // Look keys up case-insensitively: the API's field names have changed before
        var fields: [String: Any] = [:]
        for (key, value) in row { fields[key.lowercased()] = value }
        func text(_ key: String) -> String? {
            switch fields[key.lowercased()] {
            case let s as String: s
            case let n as NSNumber: n.stringValue
            default: nil
            }
        }
        func number(_ key: String) -> Double? { text(key).flatMap(Double.init) }

        name = text("LocationName") ?? "Storm overflow"
        id = text("PermitNumber") ?? text("Id") ?? name
        watercourse = text("ReceivingWaterCourse") ?? ""
        let rawStatus = (text("AlertStatus") ?? "").lowercased()
        status = rawStatus.hasPrefix("discharg") ? .discharging
            : rawStatus.contains("not") ? .notDischarging
            : rawStatus.contains("offline") ? .offline : .unknown
        statusSince = FlexibleDate.parse(text("StatusChange"))
        lastStart = FlexibleDate.parse(text("MostRecentDischargeAlertStart"))
        lastStop = FlexibleDate.parse(text("MostRecentDischargeAlertStop"))
        let past = fields["alertpast48hours"]
        dischargedInPast48h = (past as? Bool) ?? ((past as? String)?.lowercased() == "true")

        if let lat = number("Latitude"), let lon = number("Longitude") {
            latitude = lat
            longitude = lon
        } else if let x = number("X"), let y = number("Y"), x > 0, y > 0 {
            let position = BritishNationalGrid.toWGS84(easting: x, northing: y)
            latitude = position.0
            longitude = position.1
        } else {
            return nil
        }
    }

    /// Self with distances filled in, if it discharges into or close to the Tideway.
    func locatedNearTideway() -> StormOverflow? {
        let (distance, place) = TidewayRiver.distance(to: coordinate)
        let water = watercourse.lowercased()
        let onTributary = TidewayRiver.tributaries.contains { water.contains($0) }
        guard distance <= 1500 || (onTributary && distance <= 6000) else { return nil }
        var copy = self
        copy.distanceToRiver = distance
        copy.nearestPlace = place
        return copy
    }
}

/// The Thames from Walton to Putney as a rough centre line, for "how close to the river".
enum TidewayRiver {
    static let line: [(name: String, lat: Double, lon: Double)] = [
        ("Walton", 51.3880, -0.4170), ("Sunbury", 51.4040, -0.4050), ("Hampton", 51.4120, -0.3700),
        ("Hampton Court", 51.4030, -0.3430), ("Kingston", 51.4120, -0.3080), ("Teddington", 51.4310, -0.3240),
        ("Twickenham", 51.4440, -0.3240), ("Richmond", 51.4570, -0.3070), ("Isleworth", 51.4710, -0.3170),
        ("Kew", 51.4870, -0.2880), ("Chiswick", 51.4730, -0.2690), ("Barnes", 51.4720, -0.2530),
        ("Chiswick Eyot", 51.4876, -0.2485), ("Hammersmith", 51.4880, -0.2300), ("Putney", 51.4670, -0.2130),
    ]

    /// Streams that flow into this stretch, so their overflows count from further away.
    static let tributaries = ["hogsmill", "crane", "beverley", "mole", "ember", "duke of northumberland",
                              "brent", "longford", "bourne", "wandle", "thames"]

    /// Metres to the nearest point of the line, and the nearest named place.
    static func distance(to c: CLLocationCoordinate2D) -> (Double, String) {
        let metresPerDegLat = 111_320.0, metresPerDegLon = 111_320.0 * cos(c.latitude * .pi / 180)
        func xy(_ lat: Double, _ lon: Double) -> (Double, Double) {
            ((lon - c.longitude) * metresPerDegLon, (lat - c.latitude) * metresPerDegLat)
        }
        var best = Double.infinity, place = line[0].name
        for (a, b) in zip(line, line.dropFirst()) {
            let (ax, ay) = xy(a.lat, a.lon), (bx, by) = xy(b.lat, b.lon)
            let dx = bx - ax, dy = by - ay
            let t = max(0, min(1, -(ax * dx + ay * dy) / max(dx * dx + dy * dy, 1)))
            let d = hypot(ax + t * dx, ay + t * dy)
            if d < best { best = d; place = t < 0.5 ? a.name : b.name }
        }
        return (best, place)
    }
}

/// Ordnance Survey grid references (eastings/northings) to GPS latitude/longitude:
/// inverse transverse Mercator on the Airy 1830 ellipsoid, then a Helmert shift from
/// OSGB36 to WGS84 (good to a few metres, plenty here).
enum BritishNationalGrid {
    static func toWGS84(easting E: Double, northing N: Double) -> (Double, Double) {
        let (lat36, lon36) = toOSGB36(easting: E, northing: N)
        return helmert(lat: lat36, lon: lon36)
    }

    private static func toOSGB36(easting E: Double, northing N: Double) -> (Double, Double) {
        let a = 6377563.396, b = 6356256.909, F0 = 0.9996012717
        let lat0 = 49.0 * .pi / 180, lon0 = -2.0 * .pi / 180, N0 = -100000.0, E0 = 400000.0
        let e2 = 1 - b * b / (a * a), n = (a - b) / (a + b)
        let n2 = n * n, n3 = n * n * n
        var lat = lat0, M = 0.0
        repeat {
            lat = (N - N0 - M) / (a * F0) + lat
            let Ma = (1 + n + 1.25 * n2 + 1.25 * n3) * (lat - lat0)
            let Mb = (3 * n + 3 * n2 + 2.625 * n3) * sin(lat - lat0) * cos(lat + lat0)
            let Mc = (1.875 * n2 + 1.875 * n3) * sin(2 * (lat - lat0)) * cos(2 * (lat + lat0))
            let Md = (35.0 / 24) * n3 * sin(3 * (lat - lat0)) * cos(3 * (lat + lat0))
            M = b * F0 * (Ma - Mb + Mc - Md)
        } while abs(N - N0 - M) >= 0.00001
        let s = sin(lat)
        let nu = a * F0 / sqrt(1 - e2 * s * s)
        let rho = a * F0 * (1 - e2) / pow(1 - e2 * s * s, 1.5)
        let eta2 = nu / rho - 1
        let t = tan(lat), t2 = t * t, t4 = t2 * t2, sec = 1 / cos(lat)
        let VII = t / (2 * rho * nu)
        let VIII = t / (24 * rho * pow(nu, 3)) * (5 + 3 * t2 + eta2 - 9 * t2 * eta2)
        let IX = t / (720 * rho * pow(nu, 5)) * (61 + 90 * t2 + 45 * t4)
        let X = sec / nu
        let XI = sec / (6 * pow(nu, 3)) * (nu / rho + 2 * t2)
        let XII = sec / (120 * pow(nu, 5)) * (5 + 28 * t2 + 24 * t4)
        let XIIA = sec / (5040 * pow(nu, 7)) * (61 + 662 * t2 + 1320 * t4 + 720 * t4 * t2)
        let dE = E - E0
        let latOut = lat - VII * pow(dE, 2) + VIII * pow(dE, 4) - IX * pow(dE, 6)
        let lonOut = lon0 + X * dE - XI * pow(dE, 3) + XII * pow(dE, 5) - XIIA * pow(dE, 7)
        return (latOut, lonOut)
    }

    private static func helmert(lat: Double, lon: Double) -> (Double, Double) {
        var a = 6377563.396, b = 6356256.909
        var e2 = 1 - b * b / (a * a)
        var nu = a / sqrt(1 - e2 * sin(lat) * sin(lat))
        let x = nu * cos(lat) * cos(lon), y = nu * cos(lat) * sin(lon), z = (1 - e2) * nu * sin(lat)
        let tx = 446.448, ty = -125.157, tz = 542.060, s = -20.4894e-6
        let arc = Double.pi / (180 * 3600)
        let rx = 0.1502 * arc, ry = 0.2470 * arc, rz = 0.8421 * arc
        let x2 = tx + (1 + s) * x - rz * y + ry * z
        let y2 = ty + rz * x + (1 + s) * y - rx * z
        let z2 = tz - ry * x + rx * y + (1 + s) * z
        a = 6378137.0; b = 6356752.3142
        e2 = 1 - b * b / (a * a)
        let p = sqrt(x2 * x2 + y2 * y2)
        var latW = atan2(z2, p * (1 - e2))
        for _ in 0..<10 {
            nu = a / sqrt(1 - e2 * sin(latW) * sin(latW))
            latW = atan2(z2 + e2 * nu * sin(latW), p)
        }
        return (latW * 180 / .pi, atan2(y2, x2) * 180 / .pi)
    }
}
