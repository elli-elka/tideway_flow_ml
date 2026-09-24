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

    /// "Tonight 6pm", "Tomorrow 6am", "Wed 6pm"
    static func label(_ date: Date) -> String {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = london
        let hour = calendar.component(.hour, from: date)
        let time = hour < 12 ? "6am" : "6pm"
        if calendar.isDateInToday(date) { return hour < 12 ? "This morning \(time)" : "Tonight \(time)" }
        if calendar.isDateInTomorrow(date) { return "Tomorrow \(time)" }
        let formatter = DateFormatter()
        formatter.timeZone = london
        formatter.dateFormat = "EEE"
        return "\(formatter.string(from: date)) \(time)"
    }

    static func shortLabel(_ date: Date) -> (day: String, time: String) {
        let formatter = DateFormatter()
        formatter.timeZone = london
        formatter.dateFormat = "EEE"
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = london
        return (formatter.string(from: date), calendar.component(.hour, from: date) < 12 ? "6am" : "6pm")
    }
}

extension Date {
    /// "5 min ago", "3 hr ago"
    var ago: String { formatted(.relative(presentation: .named)) }
}
