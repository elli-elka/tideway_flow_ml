import CoreLocation
import Foundation

/// Sunrise, sunset and twilight times calculated on the device (the standard solar
/// position equations used by NOAA and SunCalc; accurate to about a minute).
///
/// First light / last light are civil twilight: the sun 6° below the horizon, when
/// there is enough light to see on the water without lights.
enum SunTimes {
    struct Day {
        let date: Date
        let firstLight: Date?   // civil dawn
        let sunrise: Date?
        let sunset: Date?
        let lastLight: Date?    // civil dusk

        var daylight: TimeInterval? {
            guard let sunrise, let sunset else { return nil }
            return sunset.timeIntervalSince(sunrise)
        }
    }

    /// Times for the UK calendar day containing `date`.
    static func day(containing date: Date, at coordinate: CLLocationCoordinate2D) -> Day {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = FlagSchedule.london
        let noon = calendar.date(bySettingHour: 12, minute: 0, second: 0, of: date) ?? date

        let rad = Double.pi / 180
        let lw = -coordinate.longitude * rad
        let phi = coordinate.latitude * rad
        let d = noon.timeIntervalSince1970 / 86400 - 0.5 + 2440588 - 2451545   // days since J2000

        let j0 = 0.0009
        let n = (d - j0 - lw / (2 * .pi)).rounded()
        let ds = j0 + lw / (2 * .pi) + n
        let m = rad * (357.5291 + 0.98560028 * ds)                              // mean anomaly
        let c = rad * (1.9148 * sin(m) + 0.02 * sin(2 * m) + 0.0003 * sin(3 * m))
        let l = m + c + rad * 102.9372 + .pi                                       // ecliptic longitude
        let e = rad * 23.4397
        let dec = asin(sin(e) * sin(l))                                            // declination
        let jNoon = 2451545 + ds + 0.0053 * sin(m) - 0.0069 * sin(2 * l)

        func date(fromJulian j: Double) -> Date {
            Date(timeIntervalSince1970: (j + 0.5 - 2440588) * 86400)
        }

        /// (rise, set) for the sun at `angle` degrees relative to the horizon.
        func times(_ angle: Double) -> (Date?, Date?) {
            let cosH = (sin(angle * rad) - sin(phi) * sin(dec)) / (cos(phi) * cos(dec))
            guard cosH >= -1, cosH <= 1 else { return (nil, nil) }   // sun never gets that high/low
            let w = acos(cosH)
            let a = j0 + (w + lw) / (2 * .pi) + n
            let jSet = 2451545 + a + 0.0053 * sin(m) - 0.0069 * sin(2 * l)
            let jRise = jNoon - (jSet - jNoon)
            return (date(fromJulian: jRise), date(fromJulian: jSet))
        }

        let (sunrise, sunset) = times(-0.833)
        let (dawn, dusk) = times(-6)
        return Day(date: noon, firstLight: dawn, sunrise: sunrise, sunset: sunset, lastLight: dusk)
    }
}
