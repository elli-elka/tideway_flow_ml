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

    private static let rad: Double = .pi / 180
    private static let j1970: Double = 2440588
    private static let j2000: Double = 2451545
    private static let j0: Double = 0.0009
    private static let secondsPerDay: Double = 86400

    private static func julianToDate(_ j: Double) -> Date {
        let days: Double = j + 0.5 - j1970
        return Date(timeIntervalSince1970: days * secondsPerDay)
    }

    /// Times for the UK calendar day containing `day`.
    static func day(containing day: Date, at coordinate: CLLocationCoordinate2D) -> Day {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = FlagSchedule.london
        let noon: Date = calendar.date(bySettingHour: 12, minute: 0, second: 0, of: day) ?? day

        let lw: Double = -coordinate.longitude * rad
        let phi: Double = coordinate.latitude * rad
        let unixDays: Double = noon.timeIntervalSince1970 / secondsPerDay
        let d: Double = unixDays - 0.5 + j1970 - j2000              // days since J2000

        let twoPi: Double = 2 * .pi
        let n: Double = (d - j0 - lw / twoPi).rounded()             // Julian cycle
        let ds: Double = j0 + lw / twoPi + n                        // approximate solar noon
        let m: Double = rad * (357.5291 + 0.98560028 * ds)          // mean anomaly
        let c1: Double = 1.9148 * sin(m)
        let c2: Double = 0.02 * sin(2 * m)
        let c3: Double = 0.0003 * sin(3 * m)
        let c: Double = rad * (c1 + c2 + c3)                        // equation of centre
        let l: Double = m + c + rad * 102.9372 + .pi                // ecliptic longitude
        let e: Double = rad * 23.4397                               // obliquity of the Earth
        let dec: Double = asin(sin(e) * sin(l))                     // declination
        let noonCorrection: Double = 0.0053 * sin(m) - 0.0069 * sin(2 * l)
        let jNoon: Double = j2000 + ds + noonCorrection

        /// (rise, set) for the sun at `angle` degrees relative to the horizon.
        func times(_ angle: Double) -> (Date?, Date?) {
            let numerator: Double = sin(angle * rad) - sin(phi) * sin(dec)
            let denominator: Double = cos(phi) * cos(dec)
            let cosH: Double = numerator / denominator
            guard cosH >= -1, cosH <= 1 else { return (nil, nil) }   // sun never gets that high/low
            let w: Double = acos(cosH)
            let a: Double = j0 + (w + lw) / twoPi + n
            let jSet: Double = j2000 + a + noonCorrection
            let jRise: Double = jNoon - (jSet - jNoon)
            return (julianToDate(jRise), julianToDate(jSet))
        }

        let (sunrise, sunset) = times(-0.833)
        let (dawn, dusk) = times(-6)
        return Day(date: noon, firstLight: dawn, sunrise: sunrise, sunset: sunset, lastLight: dusk)
    }
}
