import SwiftUI
import UIKit

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
        // A touch lighter in dark mode so it doesn't vanish into the background
        case .black: Color(uiColor: UIColor { $0.userInterfaceStyle == .dark
            ? UIColor(white: 0.24, alpha: 1) : UIColor(white: 0.12, alpha: 1) })
        case .green: Color(red: 0.13, green: 0.62, blue: 0.33)
        case .yellow: Color(red: 0.98, green: 0.78, blue: 0.10)
        case .red: Color(red: 0.86, green: 0.18, blue: 0.16)
        }
    }

    /// Outline that keeps the flag visible on any background: a light halo around
    /// black (only noticeable in dark mode), nothing for the other colours.
    var halo: Color { self == .black ? Color.primary.opacity(0.6) : .clear }

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

/// Flag symbol in a flag's colour, outlined so black stays visible in dark mode.
struct FlagGlyph: View {
    let colour: FlagColour?
    var size: CGFloat = 30

    var body: some View {
        Image(systemName: "flag.fill")
            .font(.system(size: size, weight: .bold))
            .foregroundStyle(colour?.color ?? .secondary)
            .overlay {
                if let colour {
                    Image(systemName: "flag")
                        .font(.system(size: size, weight: .bold))
                        .foregroundStyle(colour.halo)
                }
            }
    }
}

/// Small coloured dot for a flag, with the same halo.
struct FlagDot: View {
    let colour: FlagColour
    var size: CGFloat = 8

    var body: some View {
        Circle()
            .fill(colour.color)
            .overlay(Circle().stroke(colour.halo, lineWidth: max(1, size / 12)))
            .frame(width: size, height: size)
    }
}
