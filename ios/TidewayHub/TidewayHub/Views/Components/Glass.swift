import SwiftUI

/// A Liquid Glass panel used for every card in the app.
struct GlassCard<Content: View>: View {
    var tint: Color? = nil
    var cornerRadius: CGFloat = 26
    @ViewBuilder var content: Content

    var body: some View {
        VStack(alignment: .leading, spacing: 8) { content }
            .padding(16)
            .frame(maxWidth: .infinity, alignment: .leading)
            .glassEffect(glass, in: .rect(cornerRadius: cornerRadius))
    }

    private var glass: Glass {
        if let tint { return .regular.tint(tint.opacity(0.45)) }
        return .regular
    }
}

/// Section title used inside cards.
struct CardHeader: View {
    let title: String
    let systemImage: String
    var trailing: String? = nil

    var body: some View {
        HStack {
            Label(title, systemImage: systemImage)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.secondary)
            Spacer()
            if let trailing {
                Text(trailing).font(.caption).foregroundStyle(.secondary)
            }
        }
    }
}

/// Soft gradient behind each screen, tinted by the current flag so the glass has
/// something to refract.
struct FlagBackground: View {
    let flag: FlagColour?

    var body: some View {
        let base = flag?.color ?? .accentColor
        LinearGradient(colors: [base.opacity(0.55), Color(red: 0.05, green: 0.2, blue: 0.35), .black.opacity(0.9)],
                       startPoint: .top, endPoint: .bottom)
            .ignoresSafeArea()
    }
}

/// Arrow pointing the way the wind is blowing (i.e. away from where it comes from).
struct WindArrow: View {
    let fromDegrees: Double
    var size: CGFloat = 18

    var body: some View {
        Image(systemName: "location.north.fill")
            .font(.system(size: size, weight: .bold))
            .rotationEffect(.degrees(fromDegrees + 180))
            .accessibilityLabel("Wind from \(Compass.point(fromDegrees))")
    }
}

extension View {
    /// Standard screen chrome: flag-tinted background behind a scrolling column.
    func screenBackground(_ flag: FlagColour?) -> some View {
        background { FlagBackground(flag: flag) }
    }
}
