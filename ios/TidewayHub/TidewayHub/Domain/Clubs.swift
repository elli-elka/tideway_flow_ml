import CoreLocation
import Foundation

/// A rowing club, for the "arrived at my club" lock-screen alert.
struct Club: Identifiable, Hashable, Sendable {
    let name: String
    let latitude: Double
    let longitude: Double
    var id: String { name }
    var coordinate: CLLocationCoordinate2D { .init(latitude: latitude, longitude: longitude) }

    /// Approximate boathouse positions along the Tideway. Good to a hundred metres or
    /// so, which is fine for a 300 m geofence; "Use my current location" in Settings
    /// is exact if yours is missing or off.
    static let tideway: [Club] = [
        Club(name: "Vesta RC", latitude: 51.4665, longitude: -0.2163),
        Club(name: "London RC", latitude: 51.4665, longitude: -0.2175),
        Club(name: "Thames RC", latitude: 51.4664, longitude: -0.2190),
        Club(name: "Imperial College BC", latitude: 51.4662, longitude: -0.2205),
        Club(name: "Putney Town RC", latitude: 51.4659, longitude: -0.2222),
        Club(name: "Fulham Reach BC", latitude: 51.4856, longitude: -0.2273),
        Club(name: "Sons of the Thames RC", latitude: 51.4893, longitude: -0.2325),
        Club(name: "Auriol Kensington RC", latitude: 51.4891, longitude: -0.2336),
        Club(name: "Furnivall SC", latitude: 51.4899, longitude: -0.2343),
        Club(name: "Tideway Scullers School", latitude: 51.4727, longitude: -0.2665),
        Club(name: "Quintin BC", latitude: 51.4721, longitude: -0.2682),
        Club(name: "Mortlake Anglian & Alpha BC", latitude: 51.4716, longitude: -0.2692),
        Club(name: "Richmond Bridge (clubs)", latitude: 51.4570, longitude: -0.3060),
    ]

    static let custom = "My location"

    /// The saved home club, if one is set.
    static var saved: Club? {
        let defaults = UserDefaults.standard
        guard let name = defaults.string(forKey: SettingsKey.homeClub) else { return nil }
        if name == custom {
            guard defaults.object(forKey: SettingsKey.homeClubLatitude) != nil else { return nil }
            return Club(name: "your club", latitude: defaults.double(forKey: SettingsKey.homeClubLatitude),
                        longitude: defaults.double(forKey: SettingsKey.homeClubLongitude))
        }
        return tideway.first { $0.name == name }
    }
}
