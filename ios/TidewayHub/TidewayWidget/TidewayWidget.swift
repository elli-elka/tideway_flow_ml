import SwiftUI
import WidgetKit

/// Home Screen and Lock Screen widgets: the flag now (from the Richmond gauge, the same
/// reading as the app's "Our check"), the next forecast issue and the next tides.
/// Reads the same feed.json as the app, so it needs no shared storage.
@main
struct TidewayWidgetBundle: WidgetBundle {
    var body: some Widget {
        FlagWidget()
    }
}

struct FlagWidget: Widget {
    var body: some WidgetConfiguration {
        StaticConfiguration(kind: "TidewayFlag", provider: FlagProvider()) { entry in
            FlagWidgetView(entry: entry)
                .containerBackground(for: .widget) {
                    LinearGradient(colors: [(entry.flag?.color ?? .gray).opacity(0.35), .clear],
                                   startPoint: .topLeading, endPoint: .bottomTrailing)
                }
        }
        .configurationDisplayName("Ebb Tide Flag")
        .description("The flag at Richmond, the next forecast and the next tides.")
        .supportedFamilies([.systemSmall, .systemMedium, .accessoryCircular, .accessoryRectangular, .accessoryInline])
    }
}

// MARK: - Timeline

struct FlagEntry: TimelineEntry {
    let date: Date
    let flag: FlagColour?
    let level: Double?
    let issuedAt: Date?
    let forecast: [PredictedIssue]
    let tides: [TideEvent]
    let isSample: Bool

    static let placeholder = FlagEntry(date: .now, flag: .green, level: 1.1, issuedAt: .now, forecast: [],
                                       tides: [], isSample: true)
}

struct FlagProvider: TimelineProvider {
    func placeholder(in context: Context) -> FlagEntry { .placeholder }

    func getSnapshot(in context: Context, completion: @escaping (FlagEntry) -> Void) {
        let offline = FeedService().loadOffline()
        completion(offline.feed.map { self.entry(from: $0, isSample: offline.source == .sample) } ?? .placeholder)
    }

    func getTimeline(in context: Context, completion: @escaping (Timeline<FlagEntry>) -> Void) {
        Task {
            let service = FeedService()
            let live = await service.loadLive(from: FeedService.defaultURL)
            let offline = service.loadOffline()
            let isSample = live.feed == nil && offline.source == .sample
            let now = Date()
            let current = (live.feed ?? offline.feed).map { self.entry(from: $0, isSample: isSample) } ?? .placeholder
            // Refresh after the next flag issue (once the pipeline has run) and every
            // 30 minutes in between; iOS may stretch this out to save battery
            let next = min(FlagSchedule.nextIssue(after: now).addingTimeInterval(30 * 60), now.addingTimeInterval(30 * 60))
            completion(Timeline(entries: [current], policy: .after(next)))
        }
    }

    private func entry(from feed: Feed, isSample: Bool) -> FlagEntry {
        let now = Date()
        return FlagEntry(date: now, flag: feed.currentFlag?.flag, level: feed.currentFlag?.levelCd,
                         issuedAt: feed.currentFlag?.issuedAt,
                         forecast: (feed.predictions?.issues ?? []).filter { $0.issueAt > now },
                         tides: (feed.tides ?? []).filter { $0.t > now }, isSample: isSample)
    }
}

// MARK: - Views

struct FlagWidgetView: View {
    @Environment(\.widgetFamily) private var family
    let entry: FlagEntry

    var body: some View {
        switch family {
        case .accessoryInline: inline
        case .accessoryCircular: circular
        case .accessoryRectangular: rectangular
        case .systemMedium: medium
        default: small
        }
    }

    private var title: String { entry.flag?.title.uppercased() ?? "NO DATA" }
    private var nextTide: TideEvent? { entry.tides.first }
    private func tideText(_ tide: TideEvent) -> String { "\(tide.isHigh ? "HW" : "LW") \(UKTime.hm(tide.t))" }

    private var inline: some View {
        Text("\(title) flag" + (nextTide.map { " · \(tideText($0))" } ?? ""))
    }

    private var circular: some View {
        ZStack {
            AccessoryWidgetBackground()
            VStack(spacing: 0) {
                Image(systemName: "flag.fill").font(.system(size: 16, weight: .bold))
                Text(entry.flag.map { String($0.title.prefix(1)) } ?? "–").font(.system(size: 13, weight: .heavy))
            }
        }
        .widgetAccentable()
    }

    private var rectangular: some View {
        VStack(alignment: .leading, spacing: 1) {
            Label("\(title) flag", systemImage: "flag.fill").font(.headline).widgetAccentable()
            if let tide = nextTide { Text("Next \(tideText(tide))").font(.caption) }
            if let next = entry.forecast.first {
                Text("Forecast \(UKTime.hm(next.issueAt)): \(next.flag.title)").font(.caption2)
            }
        }
    }

    private var small: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                FlagGlyph(colour: entry.flag, size: 22)
                Spacer()
                if let issued = entry.issuedAt {
                    Text(UKTime.hm(issued)).font(.caption2).foregroundStyle(.secondary)
                }
            }
            Text(title).font(.system(size: 26, weight: .heavy, design: .rounded))
                .minimumScaleFactor(0.6).lineLimit(1)
            if let level = entry.level {
                Text("Low water \(level, format: .number.precision(.fractionLength(2))) m")
                    .font(.caption2).foregroundStyle(.secondary)
            }
            Spacer(minLength: 0)
            if let tide = nextTide {
                Label(tideText(tide), systemImage: tide.isHigh ? "arrow.up.to.line" : "arrow.down.to.line")
                    .font(.caption.weight(.semibold))
            }
            Text(entry.isSample ? "Sample data" : "Richmond gauge · unofficial")
                .font(.system(size: 9)).foregroundStyle(.secondary)
        }
    }

    private var medium: some View {
        HStack(alignment: .top, spacing: 14) {
            small.frame(maxWidth: .infinity, alignment: .leading)
            VStack(alignment: .leading, spacing: 6) {
                Text("FORECAST · EXPERIMENTAL").font(.system(size: 9, weight: .bold)).foregroundStyle(.orange)
                ForEach(entry.forecast.prefix(3)) { issue in
                    HStack(spacing: 6) {
                        FlagDot(colour: issue.flag, size: 10)
                        Text(FlagSchedule.label(issue.issueAt)).font(.caption2)
                        Spacer(minLength: 0)
                        Text(issue.flag.title).font(.caption2.weight(.semibold))
                    }
                }
                Divider()
                ForEach(entry.tides.prefix(2)) { tide in
                    HStack(spacing: 6) {
                        Image(systemName: tide.isHigh ? "arrow.up.to.line" : "arrow.down.to.line")
                            .font(.caption2)
                        Text(tideText(tide)).font(.caption2)
                        Spacer(minLength: 0)
                        if tide.predictedCd.isFinite {
                            Text("\(tide.predictedCd, format: .number.precision(.fractionLength(1))) m")
                                .font(.caption2).foregroundStyle(.secondary)
                        }
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }
}

#Preview(as: .systemMedium) {
    FlagWidget()
} timeline: {
    FlagEntry.placeholder
}
