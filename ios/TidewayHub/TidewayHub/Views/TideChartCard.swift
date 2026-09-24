import Charts
import SwiftUI

/// Richmond tide like the PLA's chart: observed, predicted and surge, scrollable
/// through time with zoom presets, and a touch-and-hold readout.
struct TideChartCard: View {
    let tide: LiveTide

    @State private var windowHours: Double = 6
    @State private var showObserved = true
    @State private var showPredicted = true
    @State private var showSurge = false
    @State private var scrollPosition = Date().addingTimeInterval(-4 * 3600)
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
            if showPredicted {
                ForEach(tide.points.filter { $0.predicted != nil }) { p in
                    LineMark(x: .value("Time", p.time), y: .value("Level", p.predicted ?? 0),
                             series: .value("Series", "Predicted"))
                        .foregroundStyle(Color.secondary)
                        .lineStyle(StrokeStyle(lineWidth: 1.5, dash: [5, 4]))
                }
            }
            if showObserved {
                ForEach(tide.points.filter { $0.observed != nil }) { p in
                    LineMark(x: .value("Time", p.time), y: .value("Level", p.observed ?? 0),
                             series: .value("Series", "Observed"))
                        .foregroundStyle(Color.cyan)
                        .lineStyle(StrokeStyle(lineWidth: 2.5))
                }
            }
            if showSurge {
                ForEach(tide.points.filter { $0.surge != nil }) { p in
                    LineMark(x: .value("Time", p.time), y: .value("Level", p.surge ?? 0),
                             series: .value("Series", "Surge"))
                        .foregroundStyle(Color.orange)
                        .lineStyle(StrokeStyle(lineWidth: 1.5))
                }
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
                        Readout(point: point)
                    }
            }
        }
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

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(UKTime.dayHM(point.time)).font(.caption2.weight(.bold))
            if let observed = point.observed { row("Observed", observed, .cyan) }
            if let predicted = point.predicted { row("Predicted", predicted, .secondary) }
            if let surge = point.surge { row("Surge", surge, .orange) }
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
