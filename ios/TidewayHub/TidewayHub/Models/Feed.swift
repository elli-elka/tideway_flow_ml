import Foundation

/// feed.json published by the pipeline (pipeline/publish_app_feed.py) to GitHub Pages.
struct Feed: Codable, Sendable {
    let version: Int
    let generatedAt: Date
    let disclaimer: String
    let officialFlagUrl: URL
    let currentFlag: FlagIssue?
    let recentFlags: [FlagIssue]
    let predictions: Predictions?
    let richmond: Richmond?
    let kingstonFlow: KingstonFlow?
    let tides: [TideEvent]?
    /// Catchment-average rain forecast for the coming week (feeds the predictions).
    let rainForecast: [RainDay]?

    // Field names are mapped explicitly (no automatic snake_case conversion, which
    // mangled names containing digits such as flow_m3s).
    enum CodingKeys: String, CodingKey {
        case version, disclaimer, predictions, richmond, tides
        case generatedAt = "generated_at"
        case officialFlagUrl = "official_flag_url"
        case currentFlag = "current_flag"
        case recentFlags = "recent_flags"
        case kingstonFlow = "kingston_flow"
        case rainForecast = "rain_forecast"
    }

    static let decoder: JSONDecoder = {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return decoder
    }()
}

struct FlagIssue: Codable, Sendable, Identifiable {
    let issuedAt: Date
    let flag: FlagColour
    let levelCd: Double
    let lowAt: Date?
    let source: String?
    var id: Date { issuedAt }

    enum CodingKeys: String, CodingKey {
        case flag, source
        case issuedAt = "issued_at"
        case levelCd = "level_cd"
        case lowAt = "low_at"
    }
}

struct Predictions: Codable, Sendable {
    let baseIssue: Date
    let modelVersion: String
    let issues: [PredictedIssue]

    enum CodingKeys: String, CodingKey {
        case issues
        case baseIssue = "base_issue"
        case modelVersion = "model_version"
    }
}

struct PredictedIssue: Codable, Sendable, Identifiable {
    let issueAt: Date
    let horizon: Int
    let flag: FlagColour
    let levelCd: Double
    let probabilities: [String: Double]
    let method: String?
    var id: Date { issueAt }

    func probability(of colour: FlagColour) -> Double { probabilities[colour.rawValue] ?? 0 }

    /// How sure the model is about its chosen flag (0–1).
    var confidence: Double { probability(of: flag) }

    enum CodingKeys: String, CodingKey {
        case horizon, flag, probabilities, method
        case issueAt = "issue_at"
        case levelCd = "level_cd"
    }
}

struct Richmond: Codable, Sendable {
    let source: String
    let latestAt: Date
    let levelCd: Double
    /// "flood" (rising) or "ebb" (falling) at the Richmond gauge.
    let stream: String?
    let series: [LevelPoint]

    enum CodingKeys: String, CodingKey {
        case source, stream, series
        case latestAt = "latest_at"
        case levelCd = "level_cd"
    }
}

struct LevelPoint: Codable, Sendable, Identifiable {
    let t: Date
    let levelCd: Double
    var id: Date { t }

    enum CodingKeys: String, CodingKey {
        case t
        case levelCd = "level_cd"
    }
}

struct KingstonFlow: Codable, Sendable {
    let latestAt: Date
    let flowM3s: Double
    let change24h: Double?
    let series: [FlowPoint]

    enum CodingKeys: String, CodingKey {
        case series
        case latestAt = "latest_at"
        case flowM3s = "flow_m3s"
        case change24h = "change_24h"
    }
}

struct FlowPoint: Codable, Sendable, Identifiable {
    let t: Date
    let flowM3s: Double
    var id: Date { t }

    enum CodingKeys: String, CodingKey {
        case t
        case flowM3s = "flow_m3s"
    }
}

struct TideEvent: Codable, Sendable, Identifiable {
    let t: Date
    let type: String
    let predictedCd: Double
    var id: Date { t }
    var isHigh: Bool { type == "high" }

    enum CodingKeys: String, CodingKey {
        case t, type
        case predictedCd = "predicted_cd"
    }
}

struct RainDay: Codable, Sendable, Identifiable {
    /// Calendar day, "yyyy-MM-dd".
    let day: String
    let catchmentMm: Double
    let chance: Double?
    var id: String { day }

    var date: Date? {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.timeZone = TimeZone(identifier: "Europe/London")
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter.date(from: day)
    }

    enum CodingKeys: String, CodingKey {
        case day, chance
        case catchmentMm = "catchment_mm"
    }
}
