import SwiftUI

enum WindUnit: String, CaseIterable, Identifiable {
    case knots = "kn", mph = "mph", metresPerSecond = "m/s"
    var id: String { rawValue }

    func value(fromKnots knots: Double) -> Double {
        switch self {
        case .knots: knots
        case .mph: knots * 1.150779
        case .metresPerSecond: knots * 0.514444
        }
    }

    func format(_ knots: Double?) -> String {
        guard let knots else { return "–" }
        return "\(Int(value(fromKnots: knots).rounded())) \(rawValue)"
    }
}

/// "12 kn gusting 20 from the SW"
func windText(_ wind: WindNow, unit: WindUnit) -> String {
    var text = unit.format(wind.speedKn)
    if let gust = wind.gustKn, gust > wind.speedKn + 2 { text += " gusting \(Int(unit.value(fromKnots: gust).rounded()))" }
    return text + " from the \(Compass.point(wind.fromDegrees))"
}

enum WeatherSymbol {
    /// WMO weather code -> SF Symbol and short description.
    static func of(_ code: Int?, night: Bool = false) -> (symbol: String, text: String) {
        switch code ?? -1 {
        case 0: (night ? "moon.stars.fill" : "sun.max.fill", "Clear")
        case 1, 2: (night ? "cloud.moon.fill" : "cloud.sun.fill", "Partly cloudy")
        case 3: ("cloud.fill", "Overcast")
        case 45, 48: ("cloud.fog.fill", "Fog")
        case 51, 53, 55, 56, 57: ("cloud.drizzle.fill", "Drizzle")
        case 61, 63, 66, 80, 81: ("cloud.rain.fill", "Rain")
        case 65, 67, 82: ("cloud.heavyrain.fill", "Heavy rain")
        case 71, 73, 75, 77, 85, 86: ("cloud.snow.fill", "Snow")
        case 95, 96, 99: ("cloud.bolt.rain.fill", "Thunderstorms")
        default: ("cloud.fill", "–")
        }
    }
}

enum FlagSchedule {
    static let london = TimeZone(identifier: "Europe/London")!

    /// The next 06:00 or 18:00 UK time after `date`, when the PLA updates the flag.
    static func nextIssue(after date: Date = .now) -> Date {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = london
        for dayOffset in 0...1 {
            guard let day = calendar.date(byAdding: .day, value: dayOffset, to: date) else { continue }
            for hour in [6, 18] {
                if let t = calendar.date(bySettingHour: hour, minute: 0, second: 0, of: day), t > date {
                    return t
                }
            }
        }
        return date.addingTimeInterval(12 * 3600)
    }

    /// "Today 18:00", "Tomorrow 06:00", "Wed 18:00"
    static func label(_ date: Date) -> String {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = london
        let time = UKTime.hm(date)
        if calendar.isDateInToday(date) { return "Today \(time)" }
        if calendar.isDateInTomorrow(date) { return "Tomorrow \(time)" }
        return "\(UKTime.weekday(date)) \(time)"
    }

    /// ("Wed", "18:00") for the prediction chips.
    static func shortLabel(_ date: Date) -> (day: String, time: String) {
        (UKTime.weekday(date), UKTime.hm(date))
    }
}

/// All times in the app: 24-hour clock, UK time, whatever the device's settings.
enum UKTime {
    private static func formatter(_ format: String) -> DateFormatter {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_GB")
        formatter.timeZone = FlagSchedule.london
        formatter.dateFormat = format
        return formatter
    }

    private static let hmFormatter = formatter("HH:mm")
    private static let weekdayFormatter = formatter("EEE")
    private static let dayHMFormatter = formatter("EEE HH:mm")

    /// "06:00"
    static func hm(_ date: Date) -> String { hmFormatter.string(from: date) }
    /// "Wed"
    static func weekday(_ date: Date) -> String { weekdayFormatter.string(from: date) }
    /// "Wed 18:00"
    static func dayHM(_ date: Date) -> String { dayHMFormatter.string(from: date) }
}

extension Date {
    /// "5 min ago", "3 hr ago"
    var ago: String { formatted(.relative(presentation: .named)) }
}
