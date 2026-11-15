import Foundation

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

    /// The latest 06:00 or 18:00 UK time at or before `date`: the flag now in force.
    static func previousIssue(before date: Date = .now) -> Date {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = london
        for dayOffset in [0, -1] {
            guard let day = calendar.date(byAdding: .day, value: dayOffset, to: date) else { continue }
            for hour in [18, 6] {
                if let t = calendar.date(bySettingHour: hour, minute: 0, second: 0, of: day), t <= date {
                    return t
                }
            }
        }
        return date.addingTimeInterval(-12 * 3600)
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
    private static let weekdayDateFormatter = formatter("EEEE d MMM")

    /// "06:00"
    static func hm(_ date: Date) -> String { hmFormatter.string(from: date) }
    /// "Wed"
    static func weekday(_ date: Date) -> String { weekdayFormatter.string(from: date) }
    /// "Wed 18:00"
    static func dayHM(_ date: Date) -> String { dayHMFormatter.string(from: date) }
    /// "Wednesday 18 Nov"
    static func weekdayDate(_ date: Date) -> String { weekdayDateFormatter.string(from: date) }
}

extension Date {
    /// "5 min ago", "3 hr ago"
    var ago: String { formatted(.relative(presentation: .named)) }
}
