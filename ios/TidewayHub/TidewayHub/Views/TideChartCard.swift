import Charts
import SwiftUI

/// Richmond tide like the PLA's chart: observed, predicted and surge, scrollable
/// through time with zoom presets. A readout above the chart always shows the time
/// and levels at the touched point (or now), and high/low waters are labelled.
struct TideChartCard: View {
    let tide: LiveTide
    /// High/low waters from other sources (the feed), for times the live data doesn't cover.
    var extraTurns: [TideEvent] = []

    @State private var windowHours: Double = 24
    @State private var showObserved = true
    @State private var showPredicted = true
    @State private var showSurge = false
    @State private var scrollPosition = Date().addingTimeInterval(-16 * 3600)
    @State private var selectedTime: Date?

    private let zoomOptions: [Double] = [3, 6, 12, 24]

    var body: some View {
        GlassCard {
            CardHeader(title: "Richmond tide", systemImage: "water.waves",
                       trailing: "\(tide.source.rawValue) · \(UKTime.hm(tide.latestObserved?.time ?? tide.fetchedAt))")

            ReadoutBar(point: shownPoint, isTouching: selectedPoint != nil, stream: tide.stream,
                       show: (showObserved, showPredicted, showSurge))

            chart
                .frame(height: 230)
                .sensoryFeedback(.selection, trigger: selectedPoint?.time)

            Picker("Zoom", selection: $windowHours) {
                ForEach(zoomOptions, id: \.self) { Text("\(Int($0))h").tag($0) }
            }
            .pickerStyle(.segmented)
            .onChange(of: windowHours) { _, hours in centreOnNow(hours) }

            HStack(spacing: 8) {
                SeriesToggle(title: "Observed", colour: .cyan, isOn: $showObserved)
                if hasPredicted { SeriesToggle(title: "Predicted", colour: .secondary, isOn: $showPredicted) }
                if hasSurge { SeriesToggle(title: "Surge", colour: .orange, isOn: $showSurge) }
            }

            Text("Swipe to move through time. Touch and hold, then slide, to read the level at any time. The flag uses the lowest level in the 12 hours before 06:00 and 18:00.")
                .font(.caption2)
                .foregroundStyle(.secondary)
        }
        .onAppear { centreOnNow(windowHours) }
    }

    /// All three series flattened into one list; built from the data only, never from
    /// the toggles, so the chart's marks stay the same as series are shown and hidden.
    private var samples: [TideSample] {
        var list: [TideSample] = []
        for p in tide.points {
            if let v = p.predicted { list.append(TideSample(time: p.time, value: v, series: .predicted)) }
            if let v = p.observed { list.append(TideSample(time: p.time, value: v, series: .observed)) }
            if let v = p.surge { list.append(TideSample(time: p.time, value: v, series: .surge)) }
        }
        return list
    }

    /// High and low waters inside the chart's time range, labelled on the chart.
    private var turnMarks: [TurnMark] {
        guard let first = tide.points.first?.time, let last = tide.points.last?.time else { return [] }
        var marks = tide.turns.map { TurnMark(time: $0.time, isHigh: $0.isHigh, level: $0.level) }
        let liveEnd = marks.last?.time ?? .distantPast
        marks += extraTurns.filter { $0.t > liveEnd.addingTimeInterval(2 * 3600) }
            .map { TurnMark(time: $0.t, isHigh: $0.isHigh, level: $0.predictedCd.isFinite ? $0.predictedCd : nil) }
        return marks.filter { $0.time >= first && $0.time <= last }.compactMap { (mark: TurnMark) -> TurnMark? in
            // Place the label on the line: the turn's own level, else the nearest reading
            if mark.level != nil { return mark }
            let near = tide.points.min { abs($0.time.timeIntervalSince(mark.time)) < abs($1.time.timeIntervalSince(mark.time)) }
            guard let level = near?.predicted ?? near?.observed else { return nil }
            return TurnMark(time: mark.time, isHigh: mark.isHigh, level: level)
        }
    }

    private func isVisible(_ series: TideSeries) -> Bool {
        switch series {
        case .observed: showObserved
        case .predicted: showPredicted
        case .surge: showSurge
        }
    }

    /// Covers every series and the flag thresholds, rounded out to half metres.
    private var yDomain: ClosedRange<Double> {
        let values = tide.points.flatMap { [$0.observed, $0.predicted, $0.surge].compactMap { $0 } }
        let low = min(values.min() ?? -0.5, -0.5), high = max(values.max() ?? 3, 3)
        return (floor(low * 2) / 2)...(ceil(high * 2) / 2 + 0.5)   // headroom for HW labels
    }

    private var hasPredicted: Bool { tide.points.contains { $0.predicted != nil } }
    private var hasSurge: Bool { tide.points.contains { $0.surge != nil } }

    private var chart: some View {
        Chart {
            ForEach(FlagLine.all) { line in
                RuleMark(y: .value("Flag threshold", line.level))
                    .foregroundStyle(line.colour.opacity(0.35))
                    .lineStyle(StrokeStyle(lineWidth: 1, dash: [3, 3]))
            }
            // Every series is always in the chart and hidden ones are just made
            // transparent: adding and removing marks confused the scrolling chart.
            ForEach(samples) { sample in
                LineMark(x: .value("Time", sample.time), y: .value("Level", sample.value),
                         series: .value("Series", sample.series.rawValue))
                    .foregroundStyle(sample.series.colour)
                    .lineStyle(sample.series.stroke)
                    .opacity(isVisible(sample.series) ? 1 : 0)
            }
            ForEach(turnMarks) { mark in
                PointMark(x: .value("Time", mark.time), y: .value("Level", mark.level ?? 0))
                    .symbolSize(30)
                    .foregroundStyle(mark.isHigh ? Color.cyan : Color.secondary)
                    .annotation(position: mark.isHigh ? .top : .bottom, spacing: 2) {
                        Text("\(mark.isHigh ? "HW" : "LW") \(UKTime.hm(mark.time))")
                            .font(.system(size: 9, weight: .semibold))
                            .foregroundStyle(.secondary)
                    }
            }
            RuleMark(x: .value("Now", Date()))
                .foregroundStyle(.primary.opacity(0.4))
                .annotation(position: .top, alignment: .center) {
                    Text("now").font(.caption2).foregroundStyle(.secondary)
                }
            if let point = selectedPoint {
                RuleMark(x: .value("Selected", point.time))
                    .foregroundStyle(.primary.opacity(0.7))
                    .lineStyle(StrokeStyle(lineWidth: 1.5))
                if showObserved, let observed = point.observed {
                    PointMark(x: .value("Time", point.time), y: .value("Level", observed))
                        .foregroundStyle(Color.cyan)
                        .symbolSize(70)
                }
                if showPredicted, let predicted = point.predicted {
                    PointMark(x: .value("Time", point.time), y: .value("Level", predicted))
                        .foregroundStyle(Color.secondary)
                        .symbolSize(50)
                }
            }
        }
        .chartYScale(domain: yDomain)   // fixed, so toggling a series doesn't rescale
        .chartYAxisLabel("m CD")
        .chartXAxis {
            AxisMarks(values: .stride(by: .hour, count: windowHours <= 6 ? 1 : (windowHours <= 12 ? 2 : 4))) { _ in
                AxisGridLine()
                AxisValueLabel(format: .dateTime.hour(.twoDigits(amPM: .omitted)).minute(.twoDigits))
            }
        }
        .chartScrollableAxes(.horizontal)
        .chartXVisibleDomain(length: windowHours * 3600)
        .chartScrollPosition(x: $scrollPosition)
        .chartXSelection(value: $selectedTime)
    }

    private var selectedPoint: TidePoint? {
        guard let selectedTime else { return nil }
        return tide.points.min { abs($0.time.timeIntervalSince(selectedTime)) < abs($1.time.timeIntervalSince(selectedTime)) }
    }

    /// The touched point, else the latest observation (or the nearest point to now).
    private var shownPoint: TidePoint? {
        if let selectedPoint { return selectedPoint }
        if let latest = tide.latestObserved { return latest }
        let now = Date()
        return tide.points.min { abs($0.time.timeIntervalSince(now)) < abs($1.time.timeIntervalSince(now)) }
    }

    /// Put "now" about two thirds of the way across the visible window.
    private func centreOnNow(_ hours: Double) {
        scrollPosition = Date().addingTimeInterval(-hours * 3600 * 0.66)
    }
}

/// The fixed readout above the chart, so values never hide behind a finger or the
/// chart's edge.
private struct ReadoutBar: View {
    let point: TidePoint?
    let isTouching: Bool
    let stream: String?
    /// Which series are switched on: observed, predicted, surge.
    let show: (Bool, Bool, Bool)

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: 12) {
            VStack(alignment: .leading, spacing: 2) {
                Text(point.map { isTouching ? UKTime.dayHM($0.time) : "Now · \(UKTime.hm($0.time))" } ?? "No readings")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(isTouching ? Color.accentColor : .secondary)
                    .monospacedDigit()
                if let level = point?.observed ?? point?.predicted {
                    Text("\(level, format: .number.precision(.fractionLength(2))) m")
                        .font(.title2.weight(.semibold))
                        .monospacedDigit()
                        .contentTransition(.numericText(value: level))
                }
            }
            Spacer(minLength: 4)
            VStack(alignment: .trailing, spacing: 2) {
                if show.0, let observed = point?.observed { row("Observed", observed, .cyan) }
                if show.1, let predicted = point?.predicted { row("Predicted", predicted, .secondary) }
                if show.2, let surge = point?.surge { row("Surge", surge, .orange, signed: true) }
                if !isTouching, let stream {
                    Label(stream == "flood" ? "Flooding" : "Ebbing",
                          systemImage: stream == "flood" ? "arrow.up.right" : "arrow.down.left")
                        .font(.caption)
                }
            }
        }
        .frame(minHeight: 52, alignment: .top)
        .animation(.snappy, value: point?.time)
    }

    private func row(_ title: String, _ value: Double, _ colour: Color, signed: Bool = false) -> some View {
        HStack(spacing: 4) {
            Circle().fill(colour).frame(width: 6, height: 6)
            Text("\(title) \(signed && value >= 0 ? "+" : "")\(value, format: .number.precision(.fractionLength(2))) m")
                .font(.caption)
                .monospacedDigit()
        }
    }
}

private struct TurnMark: Identifiable {
    let time: Date
    let isHigh: Bool
    let level: Double?
    var id: Date { time }
}

private enum TideSeries: String {
    case observed = "Observed", predicted = "Predicted", surge = "Surge"

    var colour: Color {
        switch self {
        case .observed: .cyan
        case .predicted: .secondary
        case .surge: .orange
        }
    }

    var stroke: StrokeStyle {
        switch self {
        case .observed: StrokeStyle(lineWidth: 2.5)
        case .predicted: StrokeStyle(lineWidth: 1.5, dash: [5, 4])
        case .surge: StrokeStyle(lineWidth: 1.5)
        }
    }
}

private struct TideSample: Identifiable {
    let time: Date
    let value: Double
    let series: TideSeries
    var id: String { "\(series.rawValue)-\(time.timeIntervalSince1970)" }
}

private struct FlagLine: Identifiable {
    let level: Double
    let colour: Color
    var id: Double { level }

    static let all = [FlagLine(level: 0, colour: .primary), FlagLine(level: 1.7, colour: FlagColour.yellow.color),
                      FlagLine(level: 2.6, colour: FlagColour.red.color)]
}

private struct SeriesToggle: View {
    let title: String
    let colour: Color
    @Binding var isOn: Bool

    var body: some View {
        Button {
            withAnimation(.snappy) { isOn.toggle() }
        } label: {
            HStack(spacing: 6) {
                Circle().fill(colour).frame(width: 8, height: 8).opacity(isOn ? 1 : 0.3)
                Text(title).font(.caption.weight(.semibold))
            }
            .padding(.horizontal, 10).padding(.vertical, 6)
        }
        .buttonStyle(.plain)
        .glassEffect(isOn ? .regular.interactive() : .clear.interactive(), in: .capsule)
        .opacity(isOn ? 1 : 0.6)
    }
}
