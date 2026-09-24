import CoreLocation
import Foundation

/// Forecast from Open-Meteo (free, no key; CC BY 4.0) using the Met Office's own
/// UKMO model, so it's the Met Office forecast without the Met Office app.
struct WeatherService: Sendable {
    /// The Met Office UKMO forecast, with any variable UKMO doesn't provide (e.g. rain
    /// probability) filled in from Open-Meteo's best-match blend of models.
    func forecast(at coordinate: CLLocationCoordinate2D) async throws -> WeatherForecast {
        async let ukmo = fetch(at: coordinate, model: "ukmo_seamless")
        async let blend = fetch(at: coordinate, model: nil)
        let primary = try? await ukmo
        let fallback = try? await blend
        switch (primary, fallback) {
        case let (p?, f?): return p.filled(from: f)
        case let (p?, nil): return p
        case let (nil, f?): return f
        default: throw URLError(.cannotLoadFromNetwork)
        }
    }

    private func fetch(at coordinate: CLLocationCoordinate2D, model: String?) async throws -> WeatherForecast {
        var components = URLComponents(string: "https://api.open-meteo.com/v1/forecast")!
        components.queryItems = [
            .init(name: "latitude", value: String(coordinate.latitude)),
            .init(name: "longitude", value: String(coordinate.longitude)),
            .init(name: "current", value: "temperature_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m,wind_direction_10m,wind_gusts_10m"),
            .init(name: "hourly", value: "temperature_2m,precipitation_probability,precipitation,weather_code,wind_speed_10m,wind_direction_10m,wind_gusts_10m"),
            .init(name: "daily", value: "weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,precipitation_probability_max,wind_speed_10m_max,wind_gusts_10m_max,sunrise,sunset"),
            .init(name: "wind_speed_unit", value: "kn"),
            .init(name: "timeformat", value: "unixtime"),
            .init(name: "timezone", value: "Europe/London"),
            .init(name: "forecast_days", value: "7"),
        ]
        if let model { components.queryItems?.append(.init(name: "models", value: model)) }
        let (data, response) = try await URLSession.shared.data(from: components.url!)
        if let http = response as? HTTPURLResponse, http.statusCode != 200 {
            throw URLError(.badServerResponse)
        }
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        return WeatherForecast(try decoder.decode(OpenMeteoResponse.self, from: data))
    }

    /// Current wind at several points in one request (used for the course map).
    func currentWind(at points: [CLLocationCoordinate2D]) async throws -> [WindNow] {
        var components = URLComponents(string: "https://api.open-meteo.com/v1/forecast")!
        components.queryItems = [
            .init(name: "latitude", value: points.map { String($0.latitude) }.joined(separator: ",")),
            .init(name: "longitude", value: points.map { String($0.longitude) }.joined(separator: ",")),
            .init(name: "current", value: "wind_speed_10m,wind_direction_10m,wind_gusts_10m"),
            .init(name: "wind_speed_unit", value: "kn"),
            .init(name: "timeformat", value: "unixtime"),
            .init(name: "models", value: "ukmo_seamless"),
        ]
        let (data, _) = try await URLSession.shared.data(from: components.url!)
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        // One location returns an object, several return an array
        let responses: [OpenMeteoResponse]
        if let many = try? decoder.decode([OpenMeteoResponse].self, from: data) {
            responses = many
        } else {
            responses = [try decoder.decode(OpenMeteoResponse.self, from: data)]
        }
        return responses.map { r in
            WindNow(speedKn: r.current?.windSpeed10m ?? 0,
                    gustKn: r.current?.windGusts10m,
                    fromDegrees: r.current?.windDirection10m ?? 0,
                    time: Date(timeIntervalSince1970: TimeInterval(r.current?.time ?? 0)))
        }
    }
}

// MARK: - App-facing models

struct WindNow: Sendable, Hashable {
    let speedKn: Double
    let gustKn: Double?
    /// Direction the wind is blowing FROM, degrees true.
    let fromDegrees: Double
    let time: Date
}

struct WeatherForecast: Sendable {
    struct Current: Sendable {
        let time: Date
        let temperature: Double?
        let feelsLike: Double?
        let precipitation: Double?
        let code: Int?
        let wind: WindNow
    }
    struct Hour: Sendable, Identifiable {
        let time: Date
        let temperature: Double?
        let rainChance: Double?
        let rain: Double?
        let code: Int?
        let windKn: Double?
        let gustKn: Double?
        let windFrom: Double?
        var id: Date { time }
    }
    struct Day: Sendable, Identifiable {
        let date: Date
        let code: Int?
        let high: Double?
        let low: Double?
        let rain: Double?
        let rainChance: Double?
        let windMaxKn: Double?
        let gustMaxKn: Double?
        let sunrise: Date?
        let sunset: Date?
        var id: Date { date }
    }

    let current: Current?
    let hours: [Hour]
    let days: [Day]

    init(current: Current?, hours: [Hour], days: [Day]) {
        self.current = current
        self.hours = hours
        self.days = days
    }

    /// Fill anything missing here with the same field from `other` (matched by time).
    func filled(from other: WeatherForecast) -> WeatherForecast {
        let otherHours = Dictionary(other.hours.map { ($0.time, $0) }, uniquingKeysWith: { a, _ in a })
        let otherDays = Dictionary(other.days.map { ($0.date, $0) }, uniquingKeysWith: { a, _ in a })

        let mergedCurrent: Current?
        if let c = current, let o = other.current {
            mergedCurrent = Current(
                time: c.time, temperature: c.temperature ?? o.temperature,
                feelsLike: c.feelsLike ?? o.feelsLike, precipitation: c.precipitation ?? o.precipitation,
                code: c.code ?? o.code,
                wind: c.wind.speedKn > 0 || c.wind.fromDegrees > 0 ? c.wind : o.wind)
        } else {
            mergedCurrent = current ?? other.current
        }

        let mergedHours = hours.isEmpty ? other.hours : hours.map { h in
            guard let o = otherHours[h.time] else { return h }
            return Hour(time: h.time, temperature: h.temperature ?? o.temperature,
                        rainChance: h.rainChance ?? o.rainChance, rain: h.rain ?? o.rain,
                        code: h.code ?? o.code, windKn: h.windKn ?? o.windKn,
                        gustKn: h.gustKn ?? o.gustKn, windFrom: h.windFrom ?? o.windFrom)
        }
        let mergedDays = days.isEmpty ? other.days : days.map { d in
            guard let o = otherDays[d.date] else { return d }
            return Day(date: d.date, code: d.code ?? o.code, high: d.high ?? o.high, low: d.low ?? o.low,
                       rain: d.rain ?? o.rain, rainChance: d.rainChance ?? o.rainChance,
                       windMaxKn: d.windMaxKn ?? o.windMaxKn, gustMaxKn: d.gustMaxKn ?? o.gustMaxKn,
                       sunrise: d.sunrise ?? o.sunrise, sunset: d.sunset ?? o.sunset)
        }
        return WeatherForecast(current: mergedCurrent, hours: mergedHours, days: mergedDays)
    }

    init(_ r: OpenMeteoResponse) {
        if let c = r.current {
            current = Current(time: Date(timeIntervalSince1970: TimeInterval(c.time)),
                              temperature: c.temperature2m, feelsLike: c.apparentTemperature,
                              precipitation: c.precipitation, code: c.weatherCode,
                              wind: WindNow(speedKn: c.windSpeed10m ?? 0, gustKn: c.windGusts10m,
                                            fromDegrees: c.windDirection10m ?? 0,
                                            time: Date(timeIntervalSince1970: TimeInterval(c.time))))
        } else {
            current = nil
        }
        if let h = r.hourly {
            hours = h.time.indices.map { i in
                Hour(time: Date(timeIntervalSince1970: TimeInterval(h.time[i])),
                     temperature: h.temperature2m?.at(i), rainChance: h.precipitationProbability?.at(i),
                     rain: h.precipitation?.at(i), code: h.weatherCode?.at(i),
                     windKn: h.windSpeed10m?.at(i), gustKn: h.windGusts10m?.at(i),
                     windFrom: h.windDirection10m?.at(i))
            }
        } else {
            hours = []
        }
        if let d = r.daily {
            days = d.time.indices.map { i in
                Day(date: Date(timeIntervalSince1970: TimeInterval(d.time[i])),
                    code: d.weatherCode?.at(i), high: d.temperature2mMax?.at(i),
                    low: d.temperature2mMin?.at(i), rain: d.precipitationSum?.at(i),
                    rainChance: d.precipitationProbabilityMax?.at(i),
                    windMaxKn: d.windSpeed10mMax?.at(i), gustMaxKn: d.windGusts10mMax?.at(i),
                    sunrise: d.sunrise?.at(i).map { Date(timeIntervalSince1970: TimeInterval($0)) },
                    sunset: d.sunset?.at(i).map { Date(timeIntervalSince1970: TimeInterval($0)) })
            }
        } else {
            days = []
        }
    }

    /// The next 24 hours from now.
    var next24Hours: [Hour] {
        let now = Date().addingTimeInterval(-3600)
        return Array(hours.filter { $0.time >= now }.prefix(24))
    }
}

// MARK: - Open-Meteo wire format

struct OpenMeteoResponse: Decodable, Sendable {
    struct Current: Decodable, Sendable {
        let time: Int
        let temperature2m: Double?
        let apparentTemperature: Double?
        let precipitation: Double?
        let weatherCode: Int?
        let windSpeed10m: Double?
        let windDirection10m: Double?
        let windGusts10m: Double?
    }
    struct Hourly: Decodable, Sendable {
        let time: [Int]
        let temperature2m: [Double?]?
        let precipitationProbability: [Double?]?
        let precipitation: [Double?]?
        let weatherCode: [Int?]?
        let windSpeed10m: [Double?]?
        let windDirection10m: [Double?]?
        let windGusts10m: [Double?]?
    }
    struct Daily: Decodable, Sendable {
        let time: [Int]
        let weatherCode: [Int?]?
        let temperature2mMax: [Double?]?
        let temperature2mMin: [Double?]?
        let precipitationSum: [Double?]?
        let precipitationProbabilityMax: [Double?]?
        let windSpeed10mMax: [Double?]?
        let windGusts10mMax: [Double?]?
        let sunrise: [Int?]?
        let sunset: [Int?]?
    }
    let current: Current?
    let hourly: Hourly?
    let daily: Daily?
}

extension Array {
    /// Element at `index` flattened, or nil if out of range.
    func at<T>(_ index: Int) -> T? where Element == T? {
        indices.contains(index) ? self[index] : nil
    }
}
