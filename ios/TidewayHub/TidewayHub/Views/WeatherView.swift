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
                        CurrentConditionsCard(current: current, unit: unit,
                                              placeName: store.location.isAuthorized ? "Your location" : "Putney")
                    }
                    if !store.observations.isEmpty {
                        ObservationsCard(observations: store.observations, unit: unit)
                    }
                    HourlyCard(hours: weather.next24Hours, unit: unit)
                    if let today = weather.days.first {
                        DaylightCard(day: today)
                    }
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
        .screenBackground(store.feed?.currentFlag?.flag)
        .navigationTitle("Weather")
        .refreshable {
            await store.refreshWeather()
            await store.refreshObservations()
        }
    }
}

private struct CurrentConditionsCard: View {
    let current: WeatherForecast.Current
    let unit: WindUnit
    let placeName: String

    var body: some View {
        let symbol = WeatherSymbol.of(current.code)
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                CardHeader(title: placeName, systemImage: "location.fill",
                           trailing: current.time.formatted(date: .omitted, time: .shortened))
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
                HStack(spacing: 10) {
                    WindArrow(fromDegrees: current.wind.fromDegrees)
                    Text(windText(current.wind, unit: unit)).font(.headline)
                }
            }
        }
    }
}

private struct ObservationsCard: View {
    let observations: [Observation]
    let unit: WindUnit

    var body: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 10) {
                CardHeader(title: "Measured wind now", systemImage: "gauge.with.dots.needle.33percent")
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
                            Text("G\(Int(unit.value(fromKnots: gust).rounded()))")
                                .font(.caption.weight(.semibold)).foregroundStyle(.orange)
                        }
                    }
                }
                Text("Airport reports, updated about every 30 minutes.")
                    .font(.caption2).foregroundStyle(.secondary)
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
                                Text(hour.time.formatted(.dateTime.hour()))
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

private struct DaylightCard: View {
    let day: WeatherForecast.Day

    var body: some View {
        GlassCard {
            HStack {
                if let sunrise = day.sunrise {
                    Label(sunrise.formatted(date: .omitted, time: .shortened), systemImage: "sunrise.fill")
                }
                Spacer()
                if let sunset = day.sunset {
                    Label(sunset.formatted(date: .omitted, time: .shortened), systemImage: "sunset.fill")
                }
            }
            .font(.headline)
            .symbolRenderingMode(.multicolor)
            Text("Boats need lights on the Tideway in darkness and poor visibility; check the Tideway Code.")
                .font(.caption).foregroundStyle(.secondary)
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
                ForEach(days) { day in
                    HStack(spacing: 12) {
                        Text(Calendar.current.isDateInToday(day.date) ? "Today" : day.date.formatted(.dateTime.weekday(.abbreviated)))
                            .frame(width: 52, alignment: .leading)
                        Image(systemName: WeatherSymbol.of(day.code).symbol)
                            .symbolRenderingMode(.multicolor)
                            .frame(width: 28)
                        Text(day.rain.map { String(format: "%.1f mm", $0) } ?? "–")
                            .font(.caption).foregroundStyle(.blue)
                            .frame(width: 58, alignment: .leading)
                        Spacer()
                        Text(unit.format(day.windMaxKn) + " · G" + gust(day.gustMaxKn))
                            .font(.caption).foregroundStyle(.secondary)
                        Text(degrees(day.low) + " / " + degrees(day.high))
                            .font(.subheadline.weight(.medium))
                            .frame(width: 72, alignment: .trailing)
                    }
                    .font(.subheadline)
                }
            }
        }
    }
}

/// "14°" or "–"
private func degrees(_ celsius: Double?) -> String {
    celsius.map { "\(Int($0.rounded()))°" } ?? "–"
}
