import SwiftUI

/// PLA Ebb Tide Flag, decided at 06:00 and 18:00 from the lowest Richmond tide reading
/// of the previous 12 hours (metres above chart datum).
enum FlagColour: String, Codable, CaseIterable, Sendable {
    case black = "BLACK"
    case green = "GREEN"
    case yellow = "YELLOW"
    case red = "RED"

    var title: String { rawValue.capitalized }

    var color: Color {
        switch self {
        case .black: Color(white: 0.12)
        case .green: Color(red: 0.13, green: 0.62, blue: 0.33)
        case .yellow: Color(red: 0.98, green: 0.78, blue: 0.10)
        case .red: Color(red: 0.86, green: 0.18, blue: 0.16)
        }
    }

    /// Text colour that reads well on top of `color`.
    var onColor: Color { self == .yellow ? .black : .white }

    var range: String {
        switch self {
        case .black: "below 0.0 m"
        case .green: "0.0 – 1.7 m"
        case .yellow: "1.7 – 2.6 m"
        case .red: "2.6 m or above"
        }
    }

    var summary: String {
        switch self {
        case .black: "Very low river flow"
        case .green: "Normal river flow"
        case .yellow: "Increased river flow"
        case .red: "High river flow"
        }
    }
}
