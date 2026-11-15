import Charts
import SwiftUI

struct WeatherView: View {
    @Environment(AppStore.self) private var store
    @AppStorage(SettingsKey.windUnit) private var unitRaw = WindUnit.knots.rawValue
    private var unit: WindUnit { WindUnit(rawValue: unitRaw) ?? .knots }

    var body: some View {
        ScrollView {
            VStack(spacing: 16) {
                if let error = store.weatherError {
                    GlassCard(tint: .orange) { Label(error, systemImage: "exclamationmark.triangle").font(.footnote) }
                }
                if let weather = store.weather {
                    if let current = weather.current {
                        CurrentConditionsCard(current: current, observations: store.observations, unit: unit,
                                              placeName: store.usingDeviceLocation ? "Your location" : "Putney")
                    }
                    NavigationLink { WindView() } label: {
                        CourseWindLink(wind: store.windHere, reach: store.reach, unit: unit)
                    }
                    .buttonStyle(.plain)
                    HourlyCard(hours: weather.next24Hours, unit: unit)
                    DailyCard(days: weather.days, unit: unit)
                    Text("Forecast: Met Office UKMO model via Open-Meteo (CC BY 4.0). Measured wind: aviationweather.gov.")
                        .font(.caption2).foregroundStyle(.secondary).multilineTextAlignment(.center)
                } else if store.weatherError == nil {
                    ProgressView().padding(.top, 80)
                }
            }
            .padding()
            .frame(maxWidth: 720)
            .frame(maxWidth: .infinity)
        }
        .screenBackground(store.displayFlag)
        .navigationTitle("Weather")
        .refreshable {
            await store.refreshWeather()
            await store.refreshObservations()
        }
    }
}

private struct CurrentConditionsCard: View {
    let current: WeatherForecast.Current
    let observations: [WindObservation]
    let unit: WindUnit
    let placeName: String

    var body: some View {
        let symbol = WeatherSymbol.of(current.code)
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                CardHeader(title: placeName, systemImage: "location.fill",
                           trailing: UKTime.hm(current.time))
                HStack(alignment: .center, spacing: 16) {
                    Image(systemName: symbol.symbol)
                        .symbolRenderingMode(.multicolor)
                        .font(.system(size: 54))
                    VStack(alignment: .leading) {
                        Text(degrees(current.temperature))
                            .font(.system(size: 52, weight: .semibold, design: .rounded))
                        Text(symbol.text + (current.feelsLike.map { " · feels " + degrees($0) } ?? ""))
                            .font(.subheadline).foregroundStyle(.secondary)
                    }
                }
                if let wind = current.wind {
                    HStack(spacing: 10) {
                        WindArrow(fromDegrees: wind.fromDegrees)
                        Text(windText(wind, unit: unit)).font(.headline)
                    }
                } else {
                    Text("Wind: no data").font(.headline).foregroundStyle(.secondary)
                }
                if !observations.isEmpty {
                    Divider().opacity(0.4)
                    MeasuredWind(observations: observations, unit: unit)
                }
            }
        }
    }
}

/// Measured wind at the airports, shown inside the "now" card.
private struct MeasuredWind: View {
    let observations: [WindObservation]
    let unit: WindUnit

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Label("Measured now", systemImage: "gauge.with.dots.needle.33percent")
                .font(.caption.weight(.semibold)).foregroundStyle(.secondary)
            ForEach(observations) { obs in
                HStack {
                    Text(obs.station).font(.subheadline.weight(.medium))
                    Spacer()
                    if let from = obs.fromDegrees {
                        WindArrow(fromDegrees: from, size: 14)
                        Text(Compass.point(from)).font(.subheadline).foregroundStyle(.secondary)
                    } else {
                        Text("Variable").font(.subheadline).foregroundStyle(.secondary)
                    }
                    Text(unit.format(obs.speedKn)).font(.subheadline.weight(.semibold))
                    if let gust = obs.gustKn {
                        Text("gust \(Int(unit.value(fromKnots: gust).rounded()))")
                            .font(.caption.weight(.semibold)).foregroundStyle(.orange)
                    }
                }
            }
            Text("Airport reports, updated about every 30 minutes.")
                .font(.caption2).foregroundStyle(.secondary)
        }
    }
}

/// Tap through to the wind map along the course.
private struct CourseWindLink: View {
    let wind: WindNow?
    let reach: Tideway.Reach
    let unit: WindUnit

    var body: some View {
        GlassCard {
            HStack(spacing: 12) {
                Image(systemName: "map.fill").font(.title2).foregroundStyle(.cyan)
                VStack(alignment: .leading, spacing: 2) {
                    Text("Wind on the course").font(.headline)
                    Text(wind.map { "\(CrewWind(wind: $0, heading: reach.upstreamBearing).description) going upstream at \(reach.from.name)" }
                         ?? "Map of the wind from Putney to Richmond")
                        .font(.caption).foregroundStyle(.secondary)
                }
                Spacer()
                Image(systemName: "chevron.right").foregroundStyle(.tertiary)
            }
        }
    }
}

private struct HourlyCard: View {
    let hours: [WeatherForecast.Hour]
    let unit: WindUnit

    var body: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                CardHeader(title: "Next 24 hours", systemImage: "clock")
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 14) {
                        ForEach(hours) { hour in
                            VStack(spacing: 6) {
                                Text(UKTime.hm(hour.time))
                                    .font(.caption).foregroundStyle(.secondary)
                                Image(systemName: WeatherSymbol.of(hour.code).symbol)
                                    .symbolRenderingMode(.multicolor)
                                Text(degrees(hour.temperature))
                                    .font(.subheadline.weight(.medium))
                                if let from = hour.windFrom { WindArrow(fromDegrees: from, size: 11) }
                                Text(hour.windKn.map { "\(Int(unit.value(fromKnots: $0).rounded()))" } ?? "–")
                                    .font(.caption.weight(.semibold))
                            }
                        }
                    }
                }
                Text("Wind and gusts (\(unit.rawValue))").font(.caption).foregroundStyle(.secondary)
                Chart {
                    ForEach(hours) { hour in
                        if let gust = hour.gustKn {
                            AreaMark(x: .value("Time", hour.time), y: .value("Gust", unit.value(fromKnots: gust)))
                                .foregroundStyle(.orange.opacity(0.25))
                                .interpolationMethod(.catmullRom)
                        }
                        if let wind = hour.windKn {
                            LineMark(x: .value("Time", hour.time), y: .value("Wind", unit.value(fromKnots: wind)))
                                .foregroundStyle(.cyan)
                                .interpolationMethod(.catmullRom)
                        }
                    }
                }
                .frame(height: 120)
                Text("Rain (mm/h)").font(.caption).foregroundStyle(.secondary)
                Chart {
                    ForEach(hours) { hour in
                        BarMark(x: .value("Time", hour.time, unit: .hour), y: .value("Rain", hour.rain ?? 0))
                            .foregroundStyle(.blue.gradient)
                    }
                }
                .frame(height: 70)
            }
        }
    }
}

private struct DailyCard: View {
    let days: [WeatherForecast.Day]
    let unit: WindUnit

    private func gust(_ knots: Double?) -> String {
        knots.map { String(Int(unit.value(fromKnots: $0).rounded())) } ?? "–"
    }

    var body: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 10) {
                CardHeader(title: "7 days", systemImage: "calendar")
                let lowest = days.compactMap(\.low).min() ?? 0
                let highest = days.compactMap(\.high).max() ?? 20
                ForEach(days) { day in
                    HStack(spacing: 10) {
                        Text(Calendar.current.isDateInToday(day.date) ? "Today" : UKTime.weekday(day.date))
                            .font(.subheadline.weight(.medium))
                            .frame(width: 48, alignment: .leading)
                        Image(systemName: WeatherSymbol.of(day.code).symbol)
                            .symbolRenderingMode(.multicolor)
                            .frame(width: 26)
                        VStack(alignment: .leading, spacing: 1) {
                            Text(unit.format(day.windMaxKn) + " · gust " + gust(day.gustMaxKn))
                                .font(.caption)
                            Text(day.rain.map { $0 >= 0.1 ? String(format: "%.1f mm rain", $0) : "Dry" } ?? "–")
                                .font(.caption2).foregroundStyle(.blue)
                        }
                        .frame(width: 92, alignment: .leading)
                        Text(degrees(day.low)).font(.caption).foregroundStyle(.secondary)
                            .frame(width: 30, alignment: .trailing)
                        TemperatureBar(low: day.low, high: day.high, lowest: lowest, highest: highest)
                            .frame(height: 6)
                        Text(degrees(day.high)).font(.caption.weight(.semibold))
                            .frame(width: 30, alignment: .leading)
                    }
                    if day.id != days.last?.id { Divider().opacity(0.3) }
                }
            }
        }
    }
}

/// Apple Weather-style bar: where the day's range sits within the week's.
private struct TemperatureBar: View {
    let low: Double?
    let high: Double?
    let lowest: Double
    let highest: Double

    var body: some View {
        GeometryReader { geo in
            let span = max(highest - lowest, 1)
            let start = ((low ?? lowest) - lowest) / span, end = ((high ?? highest) - lowest) / span
            ZStack(alignment: .leading) {
                Capsule().fill(.secondary.opacity(0.2))
                Capsule()
                    .fill(LinearGradient(colors: [.cyan, .yellow, .orange], startPoint: .leading, endPoint: .trailing))
                    .frame(width: max(geo.size.width * (end - start), 6))
                    .offset(x: geo.size.width * start)
            }
        }
    }
}

/// "14°" or "–"
private func degrees(_ celsius: Double?) -> String {
    celsius.map { "\(Int($0.rounded()))°" } ?? "–"
}
