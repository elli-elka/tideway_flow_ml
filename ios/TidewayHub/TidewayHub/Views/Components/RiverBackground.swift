import SwiftUI

/// Living background behind every screen: a slowly drifting mesh gradient in
/// Tideway colours, tinted by the current flag, with waves rolling along the bottom.
/// Liquid Glass needs colour and movement behind it to look its best.
struct RiverBackground: View {
    let flag: FlagColour?
    @Environment(\.colorScheme) private var scheme
    @Environment(\.accessibilityReduceMotion) private var reduceMotion

    var body: some View {
        TimelineView(.animation(minimumInterval: 1 / 30, paused: reduceMotion)) { timeline in
            let t = reduceMotion ? 0 : timeline.date.timeIntervalSinceReferenceDate
            ZStack {
                MeshGradient(width: 3, height: 3, points: points(t), colors: colors)
                Waves(time: t, dark: scheme == .dark)
            }
        }
        .ignoresSafeArea()
    }

    /// Inner mesh points wander slowly so the colours breathe.
    private func points(_ t: Double) -> [SIMD2<Float>] {
        func wobble(_ speed: Double, _ phase: Double, _ amount: Float = 0.08) -> Float {
            Float(sin(t * speed + phase)) * amount
        }
        return [
            [0, 0], [0.5 + wobble(0.21, 0), 0], [1, 0],
            [0, 0.45 + wobble(0.17, 1)], [0.5 + wobble(0.23, 2), 0.5 + wobble(0.19, 3)], [1, 0.55 + wobble(0.15, 4)],
            [0, 1], [0.5 + wobble(0.13, 5), 1], [1, 1],
        ]
    }

    private var colors: [Color] {
        let tint = flag?.color ?? Color(red: 0.2, green: 0.7, blue: 0.8)
        if scheme == .dark {
            return [
                Color(red: 0.10, green: 0.16, blue: 0.34), Color(red: 0.16, green: 0.20, blue: 0.42), Color(red: 0.36, green: 0.22, blue: 0.40),
                Color(red: 0.05, green: 0.34, blue: 0.46), tint.mix(with: .black, by: 0.45), Color(red: 0.12, green: 0.28, blue: 0.55),
                Color(red: 0.03, green: 0.18, blue: 0.28), Color(red: 0.04, green: 0.25, blue: 0.38), tint.mix(with: .black, by: 0.6),
            ]
        }
        return [
            Color(red: 0.80, green: 0.91, blue: 1.00), Color(red: 0.95, green: 0.97, blue: 1.00), Color(red: 1.00, green: 0.89, blue: 0.80),
            Color(red: 0.58, green: 0.85, blue: 0.94), tint.mix(with: .white, by: 0.55), Color(red: 0.66, green: 0.80, blue: 0.99),
            Color(red: 0.24, green: 0.62, blue: 0.76), Color(red: 0.17, green: 0.49, blue: 0.70), tint.mix(with: Color(red: 0.2, green: 0.55, blue: 0.7), by: 0.5),
        ]
    }
}

/// Three translucent sine-wave layers drifting at different speeds.
private struct Waves: View {
    let time: Double
    let dark: Bool

    var body: some View {
        Canvas { context, size in
            let layers: [(height: Double, amplitude: Double, length: Double, speed: Double, opacity: Double)] = [
                (0.78, 14, 1.3, 0.35, 0.16),
                (0.84, 10, 0.9, -0.5, 0.13),
                (0.90, 8, 0.6, 0.7, 0.10),
            ]
            for layer in layers {
                var path = Path()
                let baseY = size.height * layer.height
                path.move(to: CGPoint(x: 0, y: size.height))
                for x in stride(from: 0.0, through: size.width, by: 4) {
                    let phase = x / size.width * 2 * .pi / layer.length + time * layer.speed
                    path.addLine(to: CGPoint(x: x, y: baseY + sin(phase) * layer.amplitude))
                }
                path.addLine(to: CGPoint(x: size.width, y: size.height))
                path.closeSubpath()
                context.fill(path, with: .color(.white.opacity(dark ? layer.opacity * 0.6 : layer.opacity)))
            }
        }
        .allowsHitTesting(false)
    }
}
