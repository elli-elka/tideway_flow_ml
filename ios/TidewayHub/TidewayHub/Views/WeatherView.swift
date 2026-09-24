import CoreLocation
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
                                              placeName: store.usingDeviceLocation ? "Your location" : "Putney")
                    }
                    if !store.observations.isEmpty {
                        ObservationsCard(observations: store.observations, unit: unit)
                    }
                    HourlyCard(hours: weather.next24Hours, unit: unit)
                    DaylightCard(coordinate: store.coordinate)
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
            }
        }
    }
}

private struct ObservationsCard: View {
    let observations: [WindObservation]
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
                        Text(Calendar.current.isDateInToday(day.date) ? "Today" : UKTime.weekday(day.date))
                            .frame(width: 52, alignment: .leading)
                        Image(systemName: WeatherSymbol.of(day.code).symbol)
                            .symbolRenderingMode(.multicolor)
                            .frame(width: 28)
                        Text(day.rain.map { String(format: "%.1f mm", $0) } ?? "–")
                            .font(.caption).foregroundStyle(.blue)
                            .frame(width: 58, alignment: .leading)
                        Spacer()
                        Text(unit.format(day.windMaxKn) + " · gust " + gust(day.gustMaxKn))
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
