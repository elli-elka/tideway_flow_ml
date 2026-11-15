import MapKit
import SwiftUI

/// Sewage: storm overflows discharging into or near the Thames from Walton to Putney,
/// live from Thames Water's overflow monitors.
struct WaterView: View {
    @Environment(AppStore.self) private var store
    @State private var showAll = false

    var body: some View {
        ScrollView {
            VStack(spacing: 16) {
                if !store.hasThamesWaterKey {
                    SetupCard()
                } else {
                    SummaryCard(overflows: store.overflows, updated: store.overflowsUpdated, error: store.overflowsError)
                    if !store.overflows.isEmpty {
                        OverflowMap(overflows: store.overflows.filter(\.isRecent))
                            .frame(height: 260)
                            .clipShape(.rect(cornerRadius: 28))
                        OverflowList(title: "Discharging now", systemImage: "exclamationmark.triangle.fill",
                                     overflows: store.overflows.filter { $0.status == .discharging },
                                     empty: "Nothing discharging near the Tideway right now.")
                        OverflowList(title: "Stopped in the last 48 hours", systemImage: "clock.arrow.circlepath",
                                     overflows: store.overflows.filter { $0.status != .discharging && $0.dischargedInPast48h },
                                     empty: "None.")
                        if showAll {
                            OverflowList(title: "All monitored overflows nearby", systemImage: "list.bullet",
                                         overflows: store.overflows.filter { !$0.isRecent }, empty: "None.")
                        } else {
                            Button("Show all \(store.overflows.count) overflows nearby") {
                                withAnimation(.snappy) { showAll = true }
                            }
                            .buttonStyle(.glass)
                        }
                    }
                }
                AdviceCard()
            }
            .padding()
            .frame(maxWidth: 720)
            .frame(maxWidth: .infinity)
        }
        .screenBackground(store.displayFlag)
        .navigationTitle("Water quality")
        .refreshable { await store.refreshOverflows() }
    }
}

private struct SummaryCard: View {
    let overflows: [StormOverflow]
    let updated: Date?
    let error: String?

    var body: some View {
        let discharging = overflows.filter { $0.status == .discharging }
        let recent = overflows.filter { $0.status != .discharging && $0.dischargedInPast48h }
        let tint: Color = !discharging.isEmpty ? .brown : (!recent.isEmpty ? .orange : .green)
        GlassCard(tint: tint) {
            CardHeader(title: "Sewage near the Tideway", systemImage: "drop.triangle.fill",
                       trailing: updated.map { "Thames Water · \(UKTime.hm($0))" })
            if let error {
                Label(error, systemImage: "exclamationmark.triangle").font(.footnote)
            } else if updated == nil {
                HStack(spacing: 8) { ProgressView(); Text("Checking overflow monitors…").font(.footnote) }
            } else {
                Text(discharging.isEmpty
                     ? (recent.isEmpty ? "No discharges in the last 48 hours" : "\(recent.count) discharged in the last 48 hours")
                     : "\(discharging.count) discharging now")
                    .font(.title2.weight(.bold))
                Text(discharging.isEmpty && recent.isEmpty
                     ? "None of the \(overflows.count) monitored storm overflows between Walton and Putney has discharged recently. The river can still carry sewage from further upstream and from Mogden after heavy rain."
                     : "Untreated sewage has gone into the river nearby. Bacteria levels stay high for a day or two after a discharge stops, longer after heavy rain.")
                    .font(.footnote).foregroundStyle(.secondary)
            }
        }
    }
}

private struct OverflowList: View {
    let title: String
    let systemImage: String
    let overflows: [StormOverflow]
    let empty: String

    var body: some View {
        GlassCard {
            CardHeader(title: title, systemImage: systemImage, trailing: overflows.isEmpty ? nil : "\(overflows.count)")
            if overflows.isEmpty {
                Text(empty).font(.footnote).foregroundStyle(.secondary)
            }
            ForEach(overflows) { overflow in
                OverflowRow(overflow: overflow)
                if overflow.id != overflows.last?.id { Divider().opacity(0.3) }
            }
        }
    }
}

private struct OverflowRow: View {
    let overflow: StormOverflow

    var body: some View {
        HStack(alignment: .top, spacing: 10) {
            Circle().fill(colour).frame(width: 10, height: 10).padding(.top, 5)
            VStack(alignment: .leading, spacing: 2) {
                Text(overflow.name).font(.subheadline.weight(.semibold))
                Text([overflow.watercourse, "near \(overflow.nearestPlace)", distance]
                    .filter { !$0.isEmpty }.joined(separator: " · "))
                    .font(.caption).foregroundStyle(.secondary)
                Text(timing).font(.caption2).foregroundStyle(.secondary)
            }
        }
    }

    private var colour: Color {
        switch overflow.status {
        case .discharging: .brown
        case .notDischarging: overflow.dischargedInPast48h ? .orange : .green
        case .offline, .unknown: .gray
        }
    }

    private var distance: String {
        overflow.distanceToRiver < 200 ? "on the river"
            : String(format: "%.1f km from the river", overflow.distanceToRiver / 1000)
    }

    private var timing: String {
        switch overflow.status {
        case .discharging:
            return "Discharging since \(overflow.lastStart.map(UKTime.dayHM) ?? overflow.statusSince.map(UKTime.dayHM) ?? "–")"
        case .notDischarging:
            if let stop = overflow.lastStop { return "Last discharge ended \(UKTime.dayHM(stop)) (\(stop.ago))" }
            return "No recent discharge recorded"
        case .offline: return "Monitor offline"
        case .unknown: return "Status unknown"
        }
    }
}

private struct OverflowMap: View {
    let overflows: [StormOverflow]

    var body: some View {
        Map(initialPosition: .region(MKCoordinateRegion(
            center: CLLocationCoordinate2D(latitude: 51.45, longitude: -0.30),
            span: MKCoordinateSpan(latitudeDelta: 0.12, longitudeDelta: 0.26)))) {
            MapPolyline(coordinates: TidewayRiver.line.map { CLLocationCoordinate2D(latitude: $0.lat, longitude: $0.lon) })
                .stroke(.cyan.opacity(0.6), lineWidth: 3)
            ForEach(overflows) { overflow in
                Marker(overflow.name, systemImage: "drop.triangle.fill", coordinate: overflow.coordinate)
                    .tint(overflow.status == .discharging ? .brown : .orange)
            }
        }
        .mapStyle(.standard(pointsOfInterest: .excludingAll))
    }
}

private struct SetupCard: View {
    var body: some View {
        GlassCard(tint: .orange) {
            Label("Thames Water key needed", systemImage: "key.fill").font(.headline)
            Text("Live storm overflow data comes from Thames Water's free Open Data API. Sign up on their API portal, create an application with access to the Open Data API to get a client ID and secret, add them to Config/Secrets.xcconfig and rebuild (see ios/README.md).")
                .font(.footnote).foregroundStyle(.secondary)
            Link(destination: URL(string: "https://docs.api.thameswater.co.uk/s/")!) {
                Label("Thames Water API portal", systemImage: "arrow.up.right.square")
            }
            .buttonStyle(.glass)
        }
    }
}

private struct AdviceCard: View {
    var body: some View {
        GlassCard {
            Label("After a discharge", systemImage: "cross.case.fill").font(.subheadline.weight(.semibold))
            VStack(alignment: .leading, spacing: 6) {
                Label("Cover cuts and blisters before you go out", systemImage: "bandage.fill")
                Label("Try not to swallow river water; take care with splashes", systemImage: "drop.fill")
                Label("Wash hands before eating or drinking", systemImage: "hands.and.sparkles.fill")
                Label("Hose down boats, blades and kit afterwards", systemImage: "shower.fill")
                Label("See a doctor if you get ill after a capsize, and mention the river", systemImage: "stethoscope")
            }
            .font(.footnote)
            Text("Overflow status is live from Thames Water's event duration monitors and can be delayed or wrong. Unofficial; not a water quality test.")
                .font(.caption2).foregroundStyle(.secondary)
        }
    }
}
