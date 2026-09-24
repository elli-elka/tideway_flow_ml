import Foundation

/// Loads feed.json from GitHub Pages, keeps the last good copy on disk, and falls back
/// to the bundled SampleFeed.json so the app always has something to show.
struct FeedService: Sendable {
    static let defaultURL = URL(string: "https://elli-elka.github.io/tideway_flow_ml/feed.json")!

    enum Source: Sendable { case live, cached, sample }

    private var cacheURL: URL {
        FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("feed.json")
    }

    /// Instant, no network: the last downloaded feed, else the bundled sample.
    func loadOffline() -> (feed: Feed?, source: Source, error: String?) {
        if let data = try? Data(contentsOf: cacheURL), let feed = try? Feed.decoder.decode(Feed.self, from: data) {
            return (feed, .cached, nil)
        }
        guard let url = Bundle.main.url(forResource: "SampleFeed", withExtension: "json") else {
            return (nil, .sample, "SampleFeed.json is missing from the app bundle.")
        }
        do {
            let feed = try Feed.decoder.decode(Feed.self, from: Data(contentsOf: url))
            return (feed, .sample, nil)
        } catch {
            return (nil, .sample, "Sample feed couldn't be read: \(error)")
        }
    }

    /// Download the live feed (15 s timeout). Returns nil feed + reason on failure.
    func loadLive(from url: URL) async -> (feed: Feed?, error: String?) {
        do {
            var request = URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData, timeoutInterval: 15)
            request.setValue("application/json", forHTTPHeaderField: "Accept")
            let (data, response) = try await URLSession.shared.data(for: request)
            if let http = response as? HTTPURLResponse, http.statusCode != 200 {
                return (nil, http.statusCode == 404
                        ? "The live feed isn't published yet (GitHub Pages returned 404)."
                        : "The live feed returned HTTP \(http.statusCode).")
            }
            let feed = try Feed.decoder.decode(Feed.self, from: data)
            try? data.write(to: cacheURL)
            return (feed, nil)
        } catch let error as DecodingError {
            return (nil, "The live feed couldn't be read: \(error)")
        } catch {
            return (nil, "Couldn't reach the live feed: \(error.localizedDescription)")
        }
    }
}
