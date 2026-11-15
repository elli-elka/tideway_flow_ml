import CoreLocation
import Charts
import SwiftUI

/// Everything about the water itself: the Richmond tide, high and low water times for
/// the next few days, Kingston flow, and daylight for planning outings.
struct RiverView: View {
    @Environment(AppStore.self) private var store

    var body: some View {
        ScrollView {
            VStack(spacing: 16) {
                if let tide = store.tide {
                    TideChartCard(tide: tide, extraTurns: store.feed?.tides ?? [])
                }
                TidesCard(events: UpcomingTides.merge(live: store.liveTide, feed: store.feed))
                KingstonCard(flow: store.kingston)
                DaylightCard(coordinate: store.coordinate)
            }
            .padding()
            .frame(maxWidth: 720)
            .frame(maxWidth: .infinity)
        }
        .screenBackground(store.displayFlag)
        .navigationTitle("River")
        .refreshable {
            await store.refreshLiveTide()
            await store.refreshKingston()
            await store.refreshFeed()
        }
    }
}

// MARK: - Tide times

/// One upcoming high or low water and where its time came from.
struct UpcomingTide: Identifiable {
    enum Source: String { case live = "PLA live", pla = "PLA", estimated = "Estimated" }
    let time: Date
    let isHigh: Bool
    let level: Double?
    let source: Source
    var id: Date { time }
}

enum UpcomingTides {
    /// PLA live times first (they only cover about a day), then the feed's times for
    /// the days after: the PLA's own prediction if the pipeline has it, otherwise its
    /// harmonic estimate from the Richmond gauge.
    static func merge(live: LiveTide?, feed: Feed?, now: Date = .now) -> [UpcomingTide] {
        var events = (live?.upcomingTurns ?? []).map {
            UpcomingTide(time: $0.time, isHigh: $0.isHigh, level: $0.level, source: .live)
        }
        let liveEnd = events.last?.time ?? now
        let feedSource: UpcomingTide.Source = feed?.tidesSource == "estimated" ? .estimated : .pla
        events += (feed?.tides ?? [])
            .filter { $0.t > now && $0.t > liveEnd.addingTimeInterval(2 * 3600) }
            .map { UpcomingTide(time: $0.t, isHigh: $0.isHigh,
                                level: $0.predictedCd.isFinite ? $0.predictedCd : nil, source: feedSource) }
        return events.sorted { $0.time < $1.time }
    }
}

private struct TideDay: Identifiable {
    let day: Date
    let title: String
    let events: [UpcomingTide]
    var id: Date { day }
}

private struct TidesCard: View {
    let events: [UpcomingTide]

    /// The next three days, grouped by UK calendar day.
    private var days: [TideDay] {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = FlagSchedule.london
        let grouped = Dictionary(grouping: events) { calendar.startOfDay(for: $0.time) }
        return grouped.keys.sorted().prefix(3).map { (day: Date) -> TideDay in
            let title = calendar.isDateInToday(day) ? "Today"
                : calendar.isDateInTomorrow(day) ? "Tomorrow"
                : UKTime.weekdayDate(day)
            return TideDay(day: day, title: title, events: grouped[day] ?? [])
        }
    }

    var body: some View {
        GlassCard {
            CardHeader(title: "High and low water · Richmond", systemImage: "arrow.up.and.down")
            if events.isEmpty {
                Text("No tide times yet").font(.subheadline).foregroundStyle(.secondary)
                Text("Pull down to refresh.").font(.caption).foregroundStyle(.secondary)
            }
            ForEach(days) { day in
                VStack(alignment: .leading, spacing: 6) {
                    Text(day.title).font(.subheadline.weight(.semibold))
                    ForEach(day.events) { tide in
                        HStack {
                            Image(systemName: tide.isHigh ? "arrow.up.to.line" : "arrow.down.to.line")
                                .foregroundStyle(tide.isHigh ? .cyan : .secondary)
                                .frame(width: 20)
                            Text(tide.isHigh ? "High" : "Low").frame(width: 40, alignment: .leading)
                            // Estimated low-water times are only good to about half an hour
                            Text((tide.source == .estimated && !tide.isHigh ? "≈ " : "") + UKTime.hm(tide.time))
                                .monospacedDigit()
                            Spacer()
                            if let level = tide.level {
                                Text("\(level, format: .number.precision(.fractionLength(1))) m")
                                    .foregroundStyle(.secondary)
                                    .monospacedDigit()
                            }
                            Text(tide.source.rawValue)
                                .font(.caption2)
                                .foregroundStyle(.tertiary)
                                .frame(width: 58, alignment: .trailing)
                        }
                        .font(.subheadline)
                    }
                }
                .padding(.vertical, 2)
            }
            if events.contains(where: { $0.source == .estimated }) {
                Text("Estimated times come from the Richmond gauge's recent tides: high water is usually within 15 minutes of the PLA's prediction, low water within about half an hour.")
                    .font(.caption2).foregroundStyle(.secondary)
            }
        }
    }
}

// MARK: - Kingston

private struct KingstonCard: View {
    let flow: KingstonFlow?

    var body: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 8) {
                CardHeader(title: "Thames flow at Kingston", systemImage: "drop.fill")
                if let flow {
                    HStack(alignment: .firstTextBaseline) {
                        Text("\(flow.flowM3s, format: .number.precision(.fractionLength(1))) m³/s")
                            .font(.title3.weight(.semibold))
                        if let change = flow.change24h {
                            Label("\(change >= 0 ? "+" : "")\(change, format: .number.precision(.fractionLength(1))) in 24 h",
                                  systemImage: change >= 0 ? "arrow.up.right" : "arrow.down.right")
                                .font(.caption).foregroundStyle(.secondary)
                        }
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
                    let stale = Date().timeIntervalSince(flow.latestAt) > 3 * 3600
                    Label("Last updated \(UKTime.dayHM(flow.latestAt))",
                          systemImage: stale ? "clock.badge.exclamationmark" : "clock")
                        .font(.caption2)
                        .foregroundStyle(stale ? .orange : .secondary)
                    Text("More water over Teddington weir means higher low waters at Richmond, and a higher flag.")
                        .font(.caption2).foregroundStyle(.secondary)
                } else {
                    Text("No data").font(.title3.weight(.semibold)).foregroundStyle(.secondary)
                    Text("The Kingston gauge hasn't reported recently.")
                        .font(.caption).foregroundStyle(.secondary)
                }
            }
        }
    }
}

// MARK: - Daylight

/// First light, sunrise, sunset and last light for today and tomorrow. After today's
/// last light, tomorrow comes first: that's what a cox planning an early outing needs.
private struct DaylightCard: View {
    let coordinate: CLLocationCoordinate2D

    var body: some View {
        let now = Date()
        let today = SunTimes.day(containing: now, at: coordinate)
        let tomorrow = SunTimes.day(containing: now.addingTimeInterval(86400), at: coordinate)
        let evening = (today.lastLight ?? today.sunset ?? now) < now
        let columns = evening ? [("Tomorrow", tomorrow), ("Today", today)] : [("Today", today), ("Tomorrow", tomorrow)]

        GlassCard {
            CardHeader(title: "Daylight", systemImage: "sun.horizon.fill")
            HStack(alignment: .top, spacing: 12) {
                ForEach(columns.indices, id: \.self) { index in
                    let title = columns[index].0
                    let day = columns[index].1
                    VStack(alignment: .leading, spacing: 8) {
                        Text(title).font(.subheadline.weight(.semibold))
                        row("First light", day.firstLight, "sun.haze")
                        row("Sunrise", day.sunrise, "sunrise.fill")
                        row("Sunset", day.sunset, "sunset.fill")
                        row("Last light", day.lastLight, "moon.haze")
                    }
                    .padding(12)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .glassEffect(index == 0 ? .regular.tint(.yellow.opacity(0.18)) : .regular,
                                 in: .rect(cornerRadius: 18))
                }
            }
            Text("First and last light are civil twilight (sun 6° below the horizon). Boats need lights on the Tideway in darkness and poor visibility; check the Tideway Code.")
                .font(.caption2).foregroundStyle(.secondary)
        }
    }

    private func row(_ title: String, _ time: Date?, _ symbol: String) -> some View {
        HStack(spacing: 6) {
            Image(systemName: symbol)
                .symbolRenderingMode(.multicolor)
                .frame(width: 20)
            Text(title).font(.caption).foregroundStyle(.secondary)
            Spacer(minLength: 4)
            Text(time.map(UKTime.hm) ?? "–").font(.subheadline.weight(.semibold)).monospacedDigit()
        }
    }
}
