import CoreLocation
import Foundation

/// The Championship Course and on to Richmond, as approximate river-centre waypoints
/// in upstream order. Good enough to tell which way the river runs where you are;
/// refine the coordinates if you want the map line to follow the banks exactly.
enum Tideway {
    struct Waypoint: Identifiable, Hashable {
        let name: String
        let latitude: Double
        let longitude: Double
        /// Shown on the wind map.
        let isStation: Bool
        var id: String { name }
        var coordinate: CLLocationCoordinate2D { .init(latitude: latitude, longitude: longitude) }
    }

    static let course: [Waypoint] = [
        .init(name: "Putney Bridge", latitude: 51.4668, longitude: -0.2130, isStation: true),
        .init(name: "Putney Embankment", latitude: 51.4667, longitude: -0.2180, isStation: false),
        .init(name: "Craven Cottage", latitude: 51.4749, longitude: -0.2217, isStation: false),
        .init(name: "Harrods", latitude: 51.4830, longitude: -0.2270, isStation: false),
        .init(name: "Hammersmith Bridge", latitude: 51.4880, longitude: -0.2300, isStation: true),
        .init(name: "St Paul's School", latitude: 51.4905, longitude: -0.2380, isStation: false),
        .init(name: "Chiswick Eyot", latitude: 51.4876, longitude: -0.2485, isStation: true),
        .init(name: "Chiswick Steps", latitude: 51.4800, longitude: -0.2560, isStation: false),
        .init(name: "Barnes Bridge", latitude: 51.4722, longitude: -0.2530, isStation: true),
        .init(name: "Mortlake", latitude: 51.4690, longitude: -0.2640, isStation: false),
        .init(name: "Chiswick Bridge", latitude: 51.4722, longitude: -0.2698, isStation: false),
        .init(name: "Kew Bridge", latitude: 51.4874, longitude: -0.2873, isStation: true),
        .init(name: "Kew Gardens", latitude: 51.4830, longitude: -0.3020, isStation: false),
        .init(name: "Isleworth", latitude: 51.4715, longitude: -0.3170, isStation: false),
        .init(name: "Richmond Lock", latitude: 51.4640, longitude: -0.3150, isStation: false),
        .init(name: "Richmond Bridge", latitude: 51.4576, longitude: -0.3066, isStation: true),
    ]

    static var stations: [Waypoint] { course.filter(\.isStation) }

    /// Used when location permission isn't given.
    static let defaultCoordinate = CLLocationCoordinate2D(latitude: 51.4667, longitude: -0.2180)

    /// Where you are on the river: the nearest stretch and its upstream bearing.
    struct Reach {
        let from: Waypoint
        let to: Waypoint
        /// Direction of travel going upstream (towards Richmond), degrees true.
        let upstreamBearing: Double
        let distanceMetres: Double
        var name: String { "\(from.name) – \(to.name)" }
    }

    static func nearestReach(to coordinate: CLLocationCoordinate2D) -> Reach {
        let here = CLLocation(latitude: coordinate.latitude, longitude: coordinate.longitude)
        var best: Reach?
        for (a, b) in zip(course, course.dropFirst()) {
            let mid = CLLocation(latitude: (a.latitude + b.latitude) / 2, longitude: (a.longitude + b.longitude) / 2)
            let distance = here.distance(from: mid)
            if best == nil || distance < best!.distanceMetres {
                best = Reach(from: a, to: b, upstreamBearing: bearing(from: a.coordinate, to: b.coordinate),
                             distanceMetres: distance)
            }
        }
        return best!
    }

    static func bearing(from a: CLLocationCoordinate2D, to b: CLLocationCoordinate2D) -> Double {
        let lat1 = a.latitude * .pi / 180, lat2 = b.latitude * .pi / 180
        let dLon = (b.longitude - a.longitude) * .pi / 180
        let y = sin(dLon) * cos(lat2)
        let x = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dLon)
        return (atan2(y, x) * 180 / .pi + 360).truncatingRemainder(dividingBy: 360)
    }
}

/// Wind split into the parts a crew feels, for a boat heading on `heading`.
struct CrewWind {
    /// Positive = headwind, negative = tailwind (knots).
    let head: Double
    /// Positive = from the right, negative = from the left, facing the direction of travel (knots).
    let cross: Double

    init(wind: WindNow, heading: Double) {
        let angle = (wind.fromDegrees - heading) * .pi / 180
        head = wind.speedKn * cos(angle)
        cross = wind.speedKn * sin(angle)
    }

    var description: String {
        let along = abs(head) < 1 ? "" : (head > 0 ? "\(Int(head.rounded())) kn head" : "\(Int((-head).rounded())) kn tail")
        let side = abs(cross) < 1 ? "" : "\(Int(abs(cross).rounded())) kn cross from the \(cross > 0 ? "right" : "left")"
        let parts = [along, side].filter { !$0.isEmpty }
        return parts.isEmpty ? "Calm" : parts.joined(separator: ", ")
    }
}

enum Compass {
    static func point(_ degrees: Double) -> String {
        let names = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                     "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
        return names[Int((degrees / 22.5).rounded()) % 16]
    }
}

/// Wind blowing against the tidal stream kicks up chop on the Tideway.
enum WindAgainstTide {
    /// `stream` is "flood" (water moving upstream) or "ebb" (downstream).
    static func warning(wind: WindNow, reach: Tideway.Reach, stream: String?) -> String? {
        guard let stream, wind.speedKn >= 8 else { return nil }
        // Component of wind blowing upstream (positive) or downstream (negative)
        let upstreamPush = -CrewWind(wind: wind, heading: reach.upstreamBearing).head
        let against = (stream == "ebb" && upstreamPush > 6) || (stream == "flood" && upstreamPush < -6)
        return against ? "Wind against the \(stream) tide: expect chop, especially in exposed reaches." : nil
    }
}
