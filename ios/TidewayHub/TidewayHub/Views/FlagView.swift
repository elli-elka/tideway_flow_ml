import Charts
import SwiftUI

struct FlagView: View {
    @Environment(AppStore.self) private var store

    var body: some View {
        ScrollView {
            VStack(spacing: 16) {
                if let feed = store.feed {
                    FeedStatusBanner(feed: feed, source: store.feedSource, error: store.feedError)
                    CurrentFlagCard(issue: feed.currentFlag, stream: feed.richmond?.stream)
                    OfficialFlagCard()
                    if let predictions = feed.predictions {
                        PredictionsCard(predictions: predictions, rain: feed.rainForecast ?? [])
                    }
                    if let richmond = feed.richmond {
                        RichmondCard(richmond: richmond)
                    }
                    HStack(alignment: .top, spacing: 16) {
                        if let flow = feed.kingstonFlow { KingstonCard(flow: flow) }
                        if let tides = feed.tides, !tides.isEmpty { TidesCard(tides: tides) }
                    }
                    DisclaimerCard(text: feed.disclaimer, url: feed.officialFlagUrl)
                } else {
                    OfficialFlagCard()
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
        .refreshable { await store.refreshFeed() }
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
                    Image(systemName: "flag.fill")
                        .font(.system(size: 30, weight: .bold))
                        .foregroundStyle(flag?.color ?? .secondary)
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
                Text("Low water \(issue.levelCd, format: .number.precision(.fractionLength(2))) m · issued \(issue.issuedAt.formatted(.dateTime.weekday(.abbreviated).hour().minute()))")
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

/// The PLA's own widget: the authoritative current flag.
private struct OfficialFlagCard: View {
    var body: some View {
        GlassCard {
            CardHeader(title: "Official PLA flag", systemImage: "checkmark.seal.fill", trailing: "pla.co.uk")
            PLAFlagWidget()
                .aspectRatio(PLAFlagWidget.aspectRatio, contentMode: .fit)
                .frame(maxWidth: PLAFlagWidget.visibleSize.width)
                .frame(maxWidth: .infinity)
                .clipShape(.rect(cornerRadius: 18))
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
            Circle()
                .fill(issue.flag.color)
                .overlay(Circle().stroke(.white.opacity(0.6), lineWidth: 1))
                .frame(width: 26, height: 26)
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
            }
            .frame(height: 10)
            HStack(spacing: 12) {
                ForEach(FlagColour.allCases, id: \.self) { colour in
                    let p = issue.probability(of: colour)
                    if p >= 0.01 {
                        Label {
                            Text(p, format: .percent.precision(.fractionLength(0)))
                        } icon: {
                            Circle().fill(colour.color).frame(width: 8, height: 8)
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

private struct RichmondCard: View {
    let richmond: Richmond

    var body: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 10) {
                CardHeader(title: "Richmond level", systemImage: "water.waves",
                           trailing: "\(richmond.latestAt.formatted(date: .omitted, time: .shortened))")
                Text("\(richmond.levelCd, format: .number.precision(.fractionLength(2))) m above chart datum")
                    .font(.title3.weight(.semibold))
                Chart {
                    ForEach(richmond.series) { point in
                        AreaMark(x: .value("Time", point.t), y: .value("Level", point.levelCd))
                            .foregroundStyle(.linearGradient(colors: [.cyan.opacity(0.35), .clear],
                                                             startPoint: .top, endPoint: .bottom))
                        LineMark(x: .value("Time", point.t), y: .value("Level", point.levelCd))
                            .foregroundStyle(.cyan)
                            .interpolationMethod(.catmullRom)
                    }
                    ForEach(FlagThreshold.all) { threshold in
                        RuleMark(y: .value("Threshold", threshold.level))
                            .foregroundStyle(threshold.colour == .black ? Color.white.opacity(0.5) : threshold.colour.color)
                            .lineStyle(StrokeStyle(lineWidth: 1, dash: [4, 3]))
                            .annotation(position: .top, alignment: .leading) {
                                Text(threshold.label)
                                    .font(.caption2).foregroundStyle(.secondary)
                            }
                    }
                }
                .chartYAxisLabel("m CD")
                .frame(height: 190)
                Text("The flag uses the lowest reading in the 12 hours before 6am/6pm.")
                    .font(.caption).foregroundStyle(.secondary)
            }
        }
    }
}

/// Lines drawn on the Richmond chart where the flag changes colour.
private struct FlagThreshold: Identifiable {
    let level: Double
    let colour: FlagColour
    let label: String
    var id: Double { level }

    static let all = [
        FlagThreshold(level: 0.0, colour: .black, label: "0 m: black below"),
        FlagThreshold(level: 1.7, colour: .yellow, label: "1.7 m: yellow"),
        FlagThreshold(level: 2.6, colour: .red, label: "2.6 m: red"),
    ]
}

private struct KingstonCard: View {
    let flow: KingstonFlow

    var body: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 8) {
                CardHeader(title: "Kingston flow", systemImage: "drop.fill")
                Text("\(flow.flowM3s, format: .number.precision(.fractionLength(1))) m³/s")
                    .font(.title3.weight(.semibold))
                if let change = flow.change24h {
                    Label("\(change >= 0 ? "+" : "")\(change, format: .number.precision(.fractionLength(1))) in 24 h",
                          systemImage: change >= 0 ? "arrow.up.right" : "arrow.down.right")
                        .font(.caption).foregroundStyle(.secondary)
                }
                Chart(flow.series) { point in
                    LineMark(x: .value("Time", point.t), y: .value("Flow", point.flowM3s))
                        .interpolationMethod(.catmullRom)
                }
                .chartXAxis(.hidden)
                .chartYAxis(.hidden)
                .frame(height: 50)
            }
        }
    }
}

private struct TidesCard: View {
    let tides: [TideEvent]

    var body: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 8) {
                CardHeader(title: "Tides (PLA)", systemImage: "arrow.up.and.down")
                ForEach(tides.prefix(4)) { tide in
                    HStack {
                        Image(systemName: tide.isHigh ? "arrow.up.to.line" : "arrow.down.to.line")
                            .foregroundStyle(tide.isHigh ? .cyan : .secondary)
                        Text(tide.t.formatted(.dateTime.weekday(.abbreviated).hour().minute()))
                        Spacer()
                        Text("\(tide.predictedCd, format: .number.precision(.fractionLength(1))) m")
                            .foregroundStyle(.secondary)
                    }
                    .font(.subheadline)
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
