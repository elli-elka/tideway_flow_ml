import Charts
import SwiftUI

struct FlagView: View {
    @Environment(AppStore.self) private var store

    var body: some View {
        ScrollView {
            VStack(spacing: 16) {
                if let feed = store.feed {
                    FeedStatusBanner(feed: feed, source: store.feedSource, error: store.feedError)
                    CurrentFlagCard(issue: feed.currentFlag, stream: store.tide?.stream ?? feed.richmond?.stream)
                    OfficialFlagCard()
                    if let predictions = feed.predictions {
                        PredictionsCard(predictions: predictions, rain: feed.rainForecast ?? [])
                    }
                    if let tide = store.tide {
                        TideChartCard(tide: tide)
                    }
                    HStack(alignment: .top, spacing: 16) {
                        KingstonCard(flow: feed.kingstonFlow)
                        TidesCard(feed: feed, live: store.liveTide)
                    }
                    DisclaimerCard(text: feed.disclaimer, url: feed.officialFlagUrl)
                } else {
                    OfficialFlagCard()
                    if let tide = store.tide { TideChartCard(tide: tide) }
                    TidesCard(feed: nil, live: store.liveTide)
                    GlassCard(tint: .orange) {
                        Label("Predictions unavailable", systemImage: "exclamationmark.triangle.fill")
                            .font(.headline)
                        Text(store.feedError ?? "Loading…").font(.footnote).foregroundStyle(.secondary)
                        Button("Try again") { Task { await store.refreshFeed() } }
                            .buttonStyle(.glass)
                    }
                }
            }
            .padding()
            .frame(maxWidth: 720)          // readable column on iPad
            .frame(maxWidth: .infinity)
        }
        .screenBackground(store.feed?.currentFlag?.flag)
        .navigationTitle("Ebb Tide Flag")
        .refreshable {
            await store.refreshFeed()
            await store.refreshLiveTide()
        }
    }
}

// MARK: - Cards

private struct FeedStatusBanner: View {
    let feed: Feed
    let source: FeedService.Source
    let error: String?

    var body: some View {
        let stale = Date().timeIntervalSince(feed.generatedAt) > 12 * 3600
        if source != .live || stale {
            GlassCard(tint: .orange) {
                Label {
                    VStack(alignment: .leading, spacing: 2) {
                        Text(source == .sample ? "Showing sample data" : "Data may be out of date")
                            .font(.subheadline.weight(.semibold))
                        Text("Feed updated \(feed.generatedAt.ago)." + (error.map { " \($0)" } ?? ""))
                            .font(.caption).foregroundStyle(.secondary)
                    }
                } icon: {
                    Image(systemName: "exclamationmark.triangle.fill")
                }
            }
        }
    }
}

private struct CurrentFlagCard: View {
    let issue: FlagIssue?
    let stream: String?

    var body: some View {
        let flag = issue?.flag
        VStack(spacing: 14) {
            HStack {
                Label("Latest from Richmond gauge", systemImage: "dot.radiowaves.left.and.right")
                    .font(.footnote.weight(.semibold))
                    .foregroundStyle(.secondary)
                Spacer()
                if let stream {
                    Label(stream == "flood" ? "Flooding" : "Ebbing",
                          systemImage: stream == "flood" ? "arrow.up.right" : "arrow.down.left")
                        .font(.caption.weight(.semibold))
                        .padding(.horizontal, 10).padding(.vertical, 5)
                        .glassEffect(.regular.interactive(), in: .capsule)
                }
            }

            ZStack(alignment: .bottom) {
                FlagGauge(level: issue?.levelCd)
                    .frame(height: 150)
                VStack(spacing: 0) {
                    FlagGlyph(colour: flag, size: 30)
                        .symbolEffect(.breathe)
                    Text(flag?.title ?? "–")
                        .font(.system(size: 46, weight: .heavy, design: .rounded))
                        .contentTransition(.numericText())
                }
                .padding(.bottom, 2)
            }

            Text(flag?.summary ?? "No flag yet")
                .font(.headline)
            if let issue {
                Text("Low water \(issue.levelCd, format: .number.precision(.fractionLength(2))) m · issued \(UKTime.dayHM(issue.issuedAt))")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            }

            HStack {
                Label("Next update \(FlagSchedule.label(FlagSchedule.nextIssue()))", systemImage: "clock")
                Spacer()
                Text(FlagSchedule.nextIssue(), style: .relative)
                    .monospacedDigit()
            }
            .font(.footnote.weight(.medium))
            .padding(.horizontal, 14).padding(.vertical, 10)
            .glassEffect(.regular, in: .capsule)
        }
        .padding(20)
        .frame(maxWidth: .infinity)
        .glassEffect(.regular.tint((flag?.color ?? .gray).opacity(0.22)), in: .rect(cornerRadius: 34))
    }
}

/// The official current flag, read from the PLA's own widget and drawn natively.
/// If the widget's flag can't be read, the widget itself is shown at its fixed size.
private struct OfficialFlagCard: View {
    @State private var flag: FlagColour?
    @State private var readAt: Date?
    @State private var failed = false

    var body: some View {
        GlassCard(tint: flag.map { $0.color.opacity(0.18) }) {
            CardHeader(title: "Official PLA flag", systemImage: "checkmark.seal.fill",
                       trailing: readAt.map { "pla.co.uk · \(UKTime.hm($0))" } ?? "pla.co.uk")
            if let flag {
                HStack(spacing: 14) {
                    FlagGlyph(colour: flag, size: 34)
                    VStack(alignment: .leading, spacing: 2) {
                        Text("\(flag.title) flag").font(.title2.weight(.bold))
                        Text("\(flag.summary) · low water \(flag.range)")
                            .font(.footnote).foregroundStyle(.secondary)
                    }
                    Spacer(minLength: 0)
                }
                .padding(.vertical, 4)
                .transition(.opacity)
            } else if !failed {
                HStack(spacing: 8) {
                    ProgressView()
                    Text("Checking the PLA widget…").font(.footnote).foregroundStyle(.secondary)
                }
            }
            // One web view throughout (so it loads once): full size only when its flag
            // couldn't be read, otherwise collapsed out of sight.
            PLAWidgetFrame { read in
                withAnimation(.snappy) {
                    if let read { flag = read; readAt = Date(); failed = false } else if flag == nil { failed = true }
                }
            }
            .frame(maxWidth: failed && flag == nil ? 420 : 1, maxHeight: failed && flag == nil ? .infinity : 1)
            .frame(maxWidth: .infinity)
            .opacity(failed && flag == nil ? 1 : 0)
            .clipShape(.rect(cornerRadius: 18))
            .accessibilityHidden(!(failed && flag == nil))
        }
        .task {
            // If the widget never finishes loading, show whatever it has
            try? await Task.sleep(for: .seconds(12))
            if flag == nil { withAnimation(.snappy) { failed = true } }
        }
    }
}

private struct PredictionsCard: View {
    let predictions: Predictions
    let rain: [RainDay]
    @State private var selected: PredictedIssue?

    var body: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                CardHeader(title: "Coming up", systemImage: "sparkles",
                           trailing: "Model \(predictions.modelVersion)")
                ScrollView(.horizontal, showsIndicators: false) {
                    GlassEffectContainer(spacing: 8) {
                        HStack(spacing: 8) {
                            ForEach(predictions.issues) { issue in
                                PredictionChip(issue: issue, isSelected: selected?.id == issue.id)
                                    .onTapGesture { withAnimation(.snappy) { selected = issue } }
                            }
                        }
                    }
                    .padding(.vertical, 2)
                }
                if let issue = selected ?? predictions.issues.first {
                    PredictionDetail(issue: issue)
                }
                if !rain.isEmpty {
                    Divider().opacity(0.4)
                    CatchmentRainStrip(days: rain)
                }
            }
        }
    }
}

/// Forecast rain over the Thames catchment for each coming day: the rain the
/// predictions are reacting to (it takes 1-5 days to reach Teddington).
private struct CatchmentRainStrip: View {
    let days: [RainDay]

    var body: some View {
        let peak = max(days.map(\.catchmentMm).max() ?? 0, 5)
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Label("Rain upstream", systemImage: "cloud.rain.fill")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
                Spacer()
                Text("\(days.reduce(0) { $0 + $1.catchmentMm }, format: .number.precision(.fractionLength(0))) mm this week")
                    .font(.caption).foregroundStyle(.secondary)
            }
            HStack(alignment: .bottom, spacing: 6) {
                ForEach(days) { day in
                    VStack(spacing: 4) {
                        Text(day.catchmentMm >= 0.5 ? "\(day.catchmentMm, format: .number.precision(.fractionLength(0)))" : "")
                            .font(.caption2.weight(.semibold))
                        Capsule()
                            .fill(.blue.gradient)
                            .frame(height: max(4, 44 * day.catchmentMm / peak))
                        Text(day.date?.formatted(.dateTime.weekday(.narrow)) ?? "")
                            .font(.caption2).foregroundStyle(.secondary)
                    }
                    .frame(maxWidth: .infinity)
                }
            }
            .frame(height: 76, alignment: .bottom)
        }
    }
}

private struct PredictionChip: View {
    let issue: PredictedIssue
    let isSelected: Bool

    var body: some View {
        let label = FlagSchedule.shortLabel(issue.issueAt)
        VStack(spacing: 6) {
            Text(label.day).font(.caption2.weight(.semibold)).foregroundStyle(.secondary)
            FlagDot(colour: issue.flag, size: 26)
                .overlay(Circle().stroke(.white.opacity(0.4), lineWidth: 1))
            Text(label.time).font(.caption.weight(.semibold))
            Text(issue.confidence, format: .percent.precision(.fractionLength(0)))
                .font(.caption2).foregroundStyle(.secondary)
        }
        .frame(width: 54)
        .padding(.vertical, 10)
        .glassEffect(isSelected ? .regular.tint(issue.flag.color.opacity(0.5)).interactive() : .regular.interactive(),
                     in: .rect(cornerRadius: 18))
    }
}

private struct PredictionDetail: View {
    let issue: PredictedIssue

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("\(FlagSchedule.label(issue.issueAt)): \(issue.flag.title), low water ≈ \(issue.levelCd, format: .number.precision(.fractionLength(2))) m")
                .font(.subheadline.weight(.semibold))
            // Probability bar across the four colours
            GeometryReader { geo in
                HStack(spacing: 2) {
                    ForEach(FlagColour.allCases, id: \.self) { colour in
                        let p = issue.probability(of: colour)
                        if p > 0.005 {
                            Rectangle().fill(colour.color).frame(width: max(geo.size.width * p - 2, 2))
                        }
                    }
                }
                .clipShape(.capsule)
                .overlay(Capsule().stroke(Color.primary.opacity(0.25), lineWidth: 1))
            }
            .frame(height: 10)
            HStack(spacing: 12) {
                ForEach(FlagColour.allCases, id: \.self) { colour in
                    let p = issue.probability(of: colour)
                    if p >= 0.01 {
                        Label {
                            Text(p, format: .percent.precision(.fractionLength(0)))
                        } icon: {
                            FlagDot(colour: colour)
                        }
                        .font(.caption)
                    }
                }
                Spacer()
                if let method = issue.method {
                    Text(method.replacingOccurrences(of: "_", with: " "))
                        .font(.caption2).foregroundStyle(.tertiary)
                }
            }
        }
    }
}

private struct KingstonCard: View {
    let flow: KingstonFlow?

    var body: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 8) {
                CardHeader(title: "Kingston flow", systemImage: "drop.fill")
                if let flow {
                    Text("\(flow.flowM3s, format: .number.precision(.fractionLength(1))) m³/s")
                        .font(.title3.weight(.semibold))
                    if let change = flow.change24h {
                        Label("\(change >= 0 ? "+" : "")\(change, format: .number.precision(.fractionLength(1))) in 24 h",
                              systemImage: change >= 0 ? "arrow.up.right" : "arrow.down.right")
                            .font(.caption).foregroundStyle(.secondary)
                    }
                    if flow.series.count > 1 {
                        Chart(flow.series) { point in
                            LineMark(x: .value("Time", point.t), y: .value("Flow", point.flowM3s))
                                .interpolationMethod(.catmullRom)
                        }
                        .chartXAxis(.hidden)
                        .chartYAxis(.hidden)
                        .frame(height: 50)
                    }
                    let stale = Date().timeIntervalSince(flow.latestAt) > 6 * 3600
                    Label("Last updated \(UKTime.dayHM(flow.latestAt))", systemImage: stale ? "clock.badge.exclamationmark" : "clock")
                        .font(.caption2)
                        .foregroundStyle(stale ? .orange : .secondary)
                } else {
                    Text("No data").font(.title3.weight(.semibold)).foregroundStyle(.secondary)
                    Text("The Kingston gauge hasn't reported recently.")
                        .font(.caption).foregroundStyle(.secondary)
                }
            }
        }
    }
}

/// Upcoming high and low waters, from the best source available: PLA live data on
/// the phone, then the feed (PLA's prediction or the pipeline's harmonic estimate).
private struct TidesCard: View {
    let feed: Feed?
    let live: LiveTide?

    private var resolved: (events: [TideEvent], label: String, estimated: Bool) {
        if let turns = live?.upcomingTurns, !turns.isEmpty {
            return (turns.map { TideEvent(t: $0.time, type: $0.isHigh ? "high" : "low", predictedCd: $0.level ?? .nan) },
                    "PLA live", false)
        }
        let upcoming = (feed?.tides ?? []).filter { $0.t > Date() }
        let estimated = feed?.tidesSource == "estimated"
        return (upcoming, estimated ? "Estimated" : "PLA", estimated)
    }

    var body: some View {
        let source = resolved
        GlassCard {
            VStack(alignment: .leading, spacing: 8) {
                CardHeader(title: "Tides", systemImage: "arrow.up.and.down",
                           trailing: source.events.isEmpty ? nil : source.label)
                if source.events.isEmpty {
                    Text("No tide times yet").font(.subheadline).foregroundStyle(.secondary)
                    Text("Pull down to refresh.").font(.caption).foregroundStyle(.secondary)
                }
                ForEach(source.events.prefix(4)) { tide in
                    HStack {
                        Image(systemName: tide.isHigh ? "arrow.up.to.line" : "arrow.down.to.line")
                            .foregroundStyle(tide.isHigh ? .cyan : .secondary)
                        // Estimated low-water times are only good to about half an hour
                        Text((source.estimated && !tide.isHigh ? "≈" : "") + UKTime.dayHM(tide.t))
                        Spacer()
                        if tide.predictedCd.isFinite {
                            Text("\(tide.predictedCd, format: .number.precision(.fractionLength(1))) m")
                                .foregroundStyle(.secondary)
                        }
                    }
                    .font(.subheadline)
                }
                if source.estimated && !source.events.isEmpty {
                    Text("From the Richmond gauge's recent tides; PLA times unavailable.")
                        .font(.caption2).foregroundStyle(.secondary)
                }
            }
        }
    }
}

private struct DisclaimerCard: View {
    let text: String
    let url: URL

    var body: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 10) {
                Label("Unofficial", systemImage: "info.circle")
                    .font(.subheadline.weight(.semibold))
                Text(text).font(.footnote).foregroundStyle(.secondary)
                Link(destination: url) {
                    Label("Official PLA Ebb Tide Flag", systemImage: "arrow.up.right.square")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.glass)
            }
        }
    }
}
