import Charts
import SwiftUI

/// Richmond tide like the PLA's chart: observed, predicted and surge, scrollable
/// through time with zoom presets, and a touch-and-hold readout.
struct TideChartCard: View {
    let tide: LiveTide

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
            summary

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

            chart
                .frame(height: 230)

            Text("Drag sideways to move through time; touch and hold for exact values. The flag uses the lowest level in the 12 hours before 06:00 and 18:00.")
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
        return (floor(low * 2) / 2)...(ceil(high * 2) / 2)
    }

    private var hasPredicted: Bool { tide.points.contains { $0.predicted != nil } }
    private var hasSurge: Bool { tide.points.contains { $0.surge != nil } }

    @ViewBuilder private var summary: some View {
        if let latest = tide.latestObserved, let level = latest.observed {
            HStack(alignment: .firstTextBaseline, spacing: 12) {
                Text("\(level, format: .number.precision(.fractionLength(2))) m")
                    .font(.title2.weight(.semibold))
                if let stream = tide.stream {
                    Label(stream == "flood" ? "Flooding" : "Ebbing",
                          systemImage: stream == "flood" ? "arrow.up.right" : "arrow.down.left")
                        .font(.subheadline)
                }
                Spacer()
                if let surge = latest.surge {
                    Text("Surge \(surge >= 0 ? "+" : "")\(surge, format: .number.precision(.fractionLength(2))) m")
                        .font(.subheadline)
                        .foregroundStyle(.orange)
                }
            }
        }
    }

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
            RuleMark(x: .value("Now", Date()))
                .foregroundStyle(.primary.opacity(0.4))
                .annotation(position: .top, alignment: .center) {
                    Text("now").font(.caption2).foregroundStyle(.secondary)
                }
            if let point = selectedPoint {
                RuleMark(x: .value("Selected", point.time))
                    .foregroundStyle(.primary.opacity(0.6))
                    .annotation(position: .top, overflowResolution: .init(x: .fit(to: .chart), y: .disabled)) {
                        Readout(point: point, show: (showObserved, showPredicted, showSurge))
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

    /// Put "now" about two thirds of the way across the visible window.
    private func centreOnNow(_ hours: Double) {
        scrollPosition = Date().addingTimeInterval(-hours * 3600 * 0.66)
    }
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

private struct Readout: View {
    let point: TidePoint
    /// Which series are switched on: observed, predicted, surge.
    let show: (Bool, Bool, Bool)

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(UKTime.dayHM(point.time)).font(.caption2.weight(.bold))
            if show.0, let observed = point.observed { row("Observed", observed, .cyan) }
            if show.1, let predicted = point.predicted { row("Predicted", predicted, .secondary) }
            if show.2, let surge = point.surge { row("Surge", surge, .orange) }
        }
        .padding(8)
        .glassEffect(.regular, in: .rect(cornerRadius: 10))
    }

    private func row(_ title: String, _ value: Double, _ colour: Color) -> some View {
        HStack(spacing: 4) {
            Circle().fill(colour).frame(width: 6, height: 6)
            Text("\(title) \(value, format: .number.precision(.fractionLength(2))) m").font(.caption2)
        }
    }
}
