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

    static let decoder: JSONDecoder = {
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
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
}

struct Predictions: Codable, Sendable {
    let baseIssue: Date
    let modelVersion: String
    let issues: [PredictedIssue]
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
}

struct Richmond: Codable, Sendable {
    let source: String
    let latestAt: Date
    let levelCd: Double
    /// "flood" (rising) or "ebb" (falling) at the Richmond gauge.
    let stream: String?
    let series: [LevelPoint]
}

struct LevelPoint: Codable, Sendable, Identifiable {
    let t: Date
    let levelCd: Double
    var id: Date { t }
}

struct KingstonFlow: Codable, Sendable {
    let latestAt: Date
    let flowM3s: Double
    let change24h: Double?
    let series: [FlowPoint]
}

struct FlowPoint: Codable, Sendable, Identifiable {
    let t: Date
    let flowM3s: Double
    var id: Date { t }
}

struct TideEvent: Codable, Sendable, Identifiable {
    let t: Date
    let type: String
    let predictedCd: Double
    var id: Date { t }
    var isHigh: Bool { type == "high" }
}
