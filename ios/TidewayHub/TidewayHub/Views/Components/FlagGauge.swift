import SwiftUI

/// Semicircular dial of the four flag bands with a marker at the latest low water.
struct FlagGauge: View {
    let level: Double?

    private static let lowest = -0.6, highest = 3.4
    private struct Band: Identifiable {
        let from: Double
        let to: Double
        let colour: FlagColour
        var id: Double { from }
    }

    private static let bands = [
        Band(from: -0.6, to: 0.0, colour: .black), Band(from: 0.0, to: 1.7, colour: .green),
        Band(from: 1.7, to: 2.6, colour: .yellow), Band(from: 2.6, to: 3.4, colour: .red),
    ]
    private let lineWidth: CGFloat = 16

    var body: some View {
        GeometryReader { geo in
            let diameter = min(geo.size.width, (geo.size.height - lineWidth) * 2)
            let radius = (diameter - lineWidth) / 2
            ZStack {
                ForEach(Self.bands) { band in
                    Circle()
                        .trim(from: fraction(band.from) + 0.002, to: fraction(band.to) - 0.002)
                        .stroke(band.colour.color, style: StrokeStyle(lineWidth: lineWidth, lineCap: .round))
                        .rotationEffect(.degrees(180))   // start the dial on the left
                        .frame(width: diameter - lineWidth, height: diameter - lineWidth)
                }
                if let level {
                    let angle = Angle.degrees(180 + 360 * fraction(level)).radians
                    Circle()
                        .fill(.white)
                        .overlay(Circle().stroke(.black.opacity(0.25), lineWidth: 1))
                        .shadow(radius: 3)
                        .frame(width: lineWidth + 10, height: lineWidth + 10)
                        .offset(x: radius * cos(angle), y: radius * sin(angle))
                        .animation(.spring(duration: 0.8), value: level)
                }
            }
            .frame(width: diameter, height: diameter)
            .position(x: geo.size.width / 2, y: geo.size.height - lineWidth / 2)
        }
        .accessibilityLabel(level.map { "Low water \(String(format: "%.2f", $0)) metres" } ?? "No reading")
    }

    /// Position around the full circle (0–0.5 is the top half) for a level.
    private func fraction(_ value: Double) -> CGFloat {
        let clamped = min(max(value, Self.lowest), Self.highest)
        return CGFloat((clamped - Self.lowest) / (Self.highest - Self.lowest) * 0.5)
    }
}
