import Foundation
import Observation

/// What each data source last returned, shown under More > Diagnostics so problems
/// can be pinned down from a screenshot.
@MainActor @Observable
final class Diagnostics {
    static let shared = Diagnostics()

    struct Entry: Identifiable {
        let source: String
        let time: Date
        let ok: Bool
        let summary: String
        let detail: String?
        var id: String { source }
    }

    private(set) var entries: [String: Entry] = [:]

    var sorted: [Entry] { entries.values.sorted { $0.source < $1.source } }

    func record(_ source: String, ok: Bool, summary: String, detail: String? = nil) {
        entries[source] = Entry(source: source, time: Date(), ok: ok, summary: summary, detail: detail)
    }
}

/// First part of a response body, for diagnostics.
func snippet(_ data: Data, limit: Int = 300) -> String {
    String(decoding: data.prefix(limit), as: UTF8.self)
}
