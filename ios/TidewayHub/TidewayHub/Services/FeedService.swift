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

    func load(from url: URL) async -> (Feed?, Source, String?) {
        do {
            var request = URLRequest(url: url)
            request.cachePolicy = .reloadIgnoringLocalCacheData
            let (data, response) = try await URLSession.shared.data(for: request)
            if let http = response as? HTTPURLResponse, http.statusCode != 200 {
                throw URLError(.badServerResponse)
            }
            let feed = try Feed.decoder.decode(Feed.self, from: data)
            try? data.write(to: cacheURL)
            return (feed, .live, nil)
        } catch {
            let message = "Couldn't load the latest feed: \(error.localizedDescription)"
            if let data = try? Data(contentsOf: cacheURL),
               let feed = try? Feed.decoder.decode(Feed.self, from: data) {
                return (feed, .cached, message)
            }
            if let url = Bundle.main.url(forResource: "SampleFeed", withExtension: "json"),
               let data = try? Data(contentsOf: url),
               let feed = try? Feed.decoder.decode(Feed.self, from: data) {
                return (feed, .sample, message)
            }
            return (nil, .sample, message)
        }
    }
}
