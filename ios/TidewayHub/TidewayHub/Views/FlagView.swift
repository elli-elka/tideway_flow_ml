import SwiftUI

/// The flag screen, in order of authority:
///   1. the official PLA flag (read from the PLA's own widget);
///   2. our own check, worked out from the Richmond gauge with the PLA's rule;
///   3. the experimental forecast for the coming week.
struct FlagView: View {
    @Environment(AppStore.self) private var store
    @State private var showModelInfo = false

    var body: some View {
        ScrollView {
            VStack(spacing: 16) {
                if let feed = store.feed {
                    FeedStatusBanner(feed: feed, source: store.feedSource, error: store.feedError)
                }
                OfficialFlagCard()
                if let feed = store.feed {
                    GaugeCheckCard(issue: feed.currentFlag, official: store.officialFlag,
                                   stream: store.tide?.stream ?? feed.richmond?.stream)
                    if let predictions = feed.predictions {
                        ForecastCard(predictions: predictions, rain: feed.rainForecast ?? [],
                                     showInfo: $showModelInfo)
                    }
                    DisclaimerCard(text: feed.disclaimer, url: feed.officialFlagUrl)
                } else {
                    GlassCard(tint: .orange) {
                        Label("Forecast unavailable", systemImage: "exclamationmark.triangle.fill")
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
        .screenBackground(store.displayFlag)
        .navigationTitle("Ebb Tide Flag")
        .toolbar {
            ToolbarItem(placement: .topBarTrailing) {
                Button("How this works", systemImage: "info.circle") { showModelInfo = true }
            }
        }
        .sheet(isPresented: $showModelInfo) { ModelInfoSheet() }
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

/// The official current flag, read from the PLA's own widget and drawn large. If the
/// widget's flag can't be read, the widget itself is shown at its fixed size.
private struct OfficialFlagCard: View {
    @Environment(AppStore.self) private var store
    @State private var failed = false

    var body: some View {
        let flag = store.officialFlag
        VStack(spacing: 12) {
            HStack {
                Label("OFFICIAL · PLA", systemImage: "checkmark.seal.fill")
                    .font(.caption.weight(.bold))
                    .padding(.horizontal, 10).padding(.vertical, 5)
                    .glassEffect(.regular.tint(.blue.opacity(0.35)), in: .capsule)
                Spacer()
                if let readAt = store.officialFlagReadAt {
                    Text("Checked \(UKTime.hm(readAt))").font(.caption).foregroundStyle(.secondary)
                }
            }
            if let flag {
                VStack(spacing: 6) {
                    FlagGlyph(colour: flag, size: 56)
                        .symbolEffect(.breathe)
                    Text(flag.title.uppercased())
                        .font(.system(size: 54, weight: .heavy, design: .rounded))
                    Text(flag.summary).font(.headline)
                    Text("Low water at Richmond \(flag.range)")
                        .font(.footnote).foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 6)
                .transition(.opacity)
            } else if !failed {
                HStack(spacing: 8) {
                    ProgressView()
                    Text("Checking the PLA's official flag…").font(.footnote).foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, minHeight: 120)
            }
            // One web view throughout (so it loads once): full size only when its flag
            // couldn't be read, otherwise collapsed out of sight.
            PLAWidgetFrame { read in
                withAnimation(.snappy) {
                    if let read {
                        store.officialFlag = read
                        store.officialFlagReadAt = Date()
                        failed = false
                    } else if store.officialFlag == nil {
                        failed = true
                    }
                }
            }
            .frame(maxWidth: showWidget ? 420 : 1, maxHeight: showWidget ? .infinity : 1)
            .frame(maxWidth: .infinity)
            .opacity(showWidget ? 1 : 0)
            .clipShape(.rect(cornerRadius: 18))
            .accessibilityHidden(!showWidget)

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
        .glassEffect(.regular.tint((flag?.color ?? .gray).opacity(0.25)), in: .rect(cornerRadius: 34))
        .task {
            // If the widget never finishes loading, show whatever it has
            try? await Task.sleep(for: .seconds(12))
            if store.officialFlag == nil { withAnimation(.snappy) { failed = true } }
        }
    }

    private var showWidget: Bool { failed && store.officialFlag == nil }
}

/// Our own reading of the Richmond gauge using the PLA's rule: a cross-check on the
/// official flag (and what the forecast starts from), never a replacement for it.
private struct GaugeCheckCard: View {
    let issue: FlagIssue?
    let official: FlagColour?
    let stream: String?

    var body: some View {
        GlassCard {
            CardHeader(title: "Our check · Richmond gauge", systemImage: "dot.radiowaves.left.and.right",
                       trailing: "unofficial")
            HStack(alignment: .center, spacing: 16) {
                FlagGauge(level: issue?.levelCd)
                    .frame(width: 130, height: 70)
                VStack(alignment: .leading, spacing: 4) {
                    HStack(spacing: 6) {
                        if let flag = issue?.flag { FlagDot(colour: flag, size: 12) }
                        Text(issue.map { "\($0.flag.title)" } ?? "No reading")
                            .font(.title3.weight(.semibold))
                    }
                    if let issue {
                        Text("Low water \(issue.levelCd, format: .number.precision(.fractionLength(2))) m · \(UKTime.dayHM(issue.issuedAt))")
                            .font(.caption).foregroundStyle(.secondary)
                    }
                    if let stream {
                        Label(stream == "flood" ? "Flooding now" : "Ebbing now",
                              systemImage: stream == "flood" ? "arrow.up.right" : "arrow.down.left")
                            .font(.caption).foregroundStyle(.secondary)
                    }
                }
                Spacer(minLength: 0)
            }
            if let official, let ours = issue?.flag {
                Label(official == ours ? "Matches the official flag" : "Differs from the official flag: go by the PLA's",
                      systemImage: official == ours ? "checkmark.circle.fill" : "exclamationmark.triangle.fill")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(official == ours ? .green : .orange)
            }
            Text("Worked out from the Environment Agency's Richmond gauge using the PLA's rule (lowest level in the 12 hours before 06:00 or 18:00). The gauge and the PLA's own sensor can differ by a few centimetres.")
                .font(.caption2).foregroundStyle(.secondary)
        }
    }
}

/// The week-ahead forecast, styled to look provisional: dashed chips, an
/// "experimental" badge, later days faded. Touch and hold for how it works.
private struct ForecastCard: View {
    let predictions: Predictions
    let rain: [RainDay]
    @Binding var showInfo: Bool
    @State private var selected: PredictedIssue?
    @State private var holdCount = 0

    var body: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                HStack {
                    Label("Forecast", systemImage: "sparkles")
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(.secondary)
                    Text("EXPERIMENTAL")
                        .font(.caption2.weight(.heavy))
                        .padding(.horizontal, 7).padding(.vertical, 3)
                        .background(Capsule().strokeBorder(.orange, style: StrokeStyle(lineWidth: 1, dash: [3, 2])))
                        .foregroundStyle(.orange)
                    Spacer()
                    Button { showInfo = true } label: { Image(systemName: "info.circle") }
                        .buttonStyle(.plain)
                        .foregroundStyle(.secondary)
                        .accessibilityLabel("How the forecast works")
                }
                ScrollView(.horizontal, showsIndicators: false) {
                    GlassEffectContainer(spacing: 8) {
                        HStack(spacing: 8) {
                            ForEach(predictions.issues) { issue in
                                PredictionChip(issue: issue, isSelected: selected?.id == issue.id,
                                               isWeekAhead: issue.issueAt.timeIntervalSince(predictions.baseIssue) > 3 * 86400)
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
                Text("A model's guess, not the PLA's. The next day or two are usually close; beyond three days (faded) treat it as a rough guide, especially after heavy rain. Touch and hold for how it works.")
                    .font(.caption2).foregroundStyle(.secondary)
            }
        }
        .onLongPressGesture(minimumDuration: 0.5) {
            holdCount += 1
            showInfo = true
        }
        .sensoryFeedback(.impact(weight: .medium), trigger: holdCount)
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
    let isWeekAhead: Bool

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
        // Dashed outline: a forecast, not a reading
        .overlay(RoundedRectangle(cornerRadius: 18)
            .strokeBorder(.secondary.opacity(0.5), style: StrokeStyle(lineWidth: 1, dash: [4, 3])))
        .glassEffect(isSelected ? .regular.tint(issue.flag.color.opacity(0.5)).interactive() : .regular.interactive(),
                     in: .rect(cornerRadius: 18))
        .opacity(isWeekAhead && !isSelected ? 0.6 : 1)
    }
}

private struct PredictionDetail: View {
    let issue: PredictedIssue

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("\(FlagSchedule.label(issue.issueAt)): probably \(issue.flag.title), low water ≈ \(issue.levelCd, format: .number.precision(.fractionLength(2))) m")
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

// MARK: - How it works

/// Plain-English explanation of where each flag on this screen comes from.
struct ModelInfoSheet: View {
    @Environment(\.dismiss) private var dismiss
    @Environment(AppStore.self) private var store

    var body: some View {
        NavigationStack {
            List {
                Section("The Ebb Tide Flag") {
                    Text("The Port of London Authority sets the flag at 06:00 and 18:00 from the lowest tide level at Richmond in the previous 12 hours, in metres above chart datum: below 0 m black, 0–1.7 m green, 1.7–2.6 m yellow, 2.6 m or more red. A high low-water means a lot of fresh water coming down the river and a strong ebb.")
                }
                Section("Official flag") {
                    Text("Read from the PLA's own embeddable flag widget each time you open the app. This is the one to go by.")
                }
                Section("Our check") {
                    Text("The app applies the same rule to the Environment Agency's Richmond gauge (converted to chart datum with an offset calibrated against the PLA's readings). It should agree with the PLA; if it doesn't, the PLA's flag wins.")
                }
                Section("The forecast") {
                    Text("Every few hours a model is retrained on the last year of flags and the things that drive them:")
                    Label("Recent low waters and their trend", systemImage: "water.waves")
                    Label("Spring and neap tides (moon phase)", systemImage: "moon.fill")
                    Label("Rain over the Thames catchment, measured and forecast", systemImage: "cloud.rain.fill")
                    Label("River flows at Kingston and upstream gauges", systemImage: "drop.fill")
                    Label("Groundwater and soil moisture", systemImage: "square.3.layers.3d.down.right")
                    Text("For each look-ahead (12 hours to 7 days) it compares three methods, \"same as now\", a linear model and a gradient-boosted tree model, by testing each month on data it hadn't seen, and uses the one that scored best. In dry spells \"same as now\" is often the most accurate, so the forecast can look flat; after heavy rain the other models take over.")
                }
                Section("How far to trust it") {
                    Text("The next day or two are usually close. Beyond three days it depends on the rain forecast, which is itself uncertain, so treat the week ahead as speculative. The percentages show how sure the model is about each colour.")
                }
                if let model = store.feed?.predictions?.modelVersion {
                    Section("Version") {
                        LabeledContent("Model", value: model)
                        if let base = store.feed?.predictions?.baseIssue {
                            LabeledContent("Forecast from", value: UKTime.dayHM(base))
                        }
                    }
                }
            }
            .navigationTitle("How this works")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) { Button("Done") { dismiss() } }
            }
        }
    }
}
