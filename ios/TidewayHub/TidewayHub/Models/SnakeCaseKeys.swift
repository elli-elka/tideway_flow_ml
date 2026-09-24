import Foundation

extension JSONDecoder.KeyDecodingStrategy {
    /// snake_case -> camelCase that leaves characters after digits alone.
    ///
    /// Foundation's `.convertFromSnakeCase` capitalises each word after an underscore
    /// with `capitalized`, which turns "2m" into "2M": Open-Meteo's "temperature_2m"
    /// became "temperature2M" and never matched `temperature2m`, so every weather value
    /// silently decoded as nil (and the feed's "flow_m3s" / "change_24h" failed too).
    /// This only uppercases the first character of each word, so "2m" stays "2m".
    static let snakeCaseKeepingDigits = JSONDecoder.KeyDecodingStrategy.custom { codingPath in
        let key = codingPath.last!.stringValue
        let words = key.split(separator: "_", omittingEmptySubsequences: true)
        guard words.count > 1 else { return PlainKey(key) }
        let camel = String(words[0]) + words.dropFirst().map { $0.prefix(1).uppercased() + $0.dropFirst() }.joined()
        return PlainKey(camel)
    }
}

private struct PlainKey: CodingKey {
    var stringValue: String
    var intValue: Int?

    init(_ string: String) { stringValue = string }
    init?(stringValue: String) { self.stringValue = stringValue }
    init?(intValue: Int) {
        stringValue = String(intValue)
        self.intValue = intValue
    }
}
