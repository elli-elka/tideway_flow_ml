import SwiftUI
import WebKit

/// The PLA's own embeddable Ebb Tide Flag widget: the official current flag.
///
/// Once the widget has loaded, its text, class names and images are read to find
/// which flag it is showing, so the app can draw that flag natively at the right
/// size. If the flag can't be read, the widget itself is shown in a fixed frame of
/// its published size (382 x 442 CSS px, bottom pixel cropped), zoomed to fit.
struct PLAFlagWidget: UIViewRepresentable {
    static let url = URL(string: "https://pla.co.uk/pla-api-integration/ebb-tide-widget-embed")!
    static let size = CGSize(width: 382, height: 442)
    /// Trimmed from the widget's edges (CSS px): the bottom row is a stray border line.
    static let crop = EdgeInsets(top: 0, leading: 0, bottom: 1, trailing: 0)

    /// Called after each read of the loaded widget: the flag, or nil if it couldn't be told.
    var onRead: @MainActor (FlagColour?) -> Void

    func makeCoordinator() -> Coordinator { Coordinator(onRead: onRead) }

    func makeUIView(context: Context) -> WKWebView {
        let web = WKWebView(frame: .zero, configuration: WKWebViewConfiguration())
        web.isOpaque = false
        web.backgroundColor = .clear
        web.scrollView.backgroundColor = .clear
        web.scrollView.isScrollEnabled = false
        web.scrollView.bounces = false
        web.navigationDelegate = context.coordinator
        web.load(URLRequest(url: Self.url))
        return web
    }

    func updateUIView(_ web: WKWebView, context: Context) {
        context.coordinator.onRead = onRead
        Coordinator.fit(web)
    }

    @MainActor
    final class Coordinator: NSObject, WKNavigationDelegate {
        var onRead: @MainActor (FlagColour?) -> Void

        init(onRead: @escaping @MainActor (FlagColour?) -> Void) { self.onRead = onRead }

        func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
            Self.fit(webView)
            // The widget may fill itself in after loading, so look a few times
            Task { @MainActor [weak self, weak webView] in
                var found: FlagColour?
                for delay in [0.3, 1.5, 3.0] {
                    try? await Task.sleep(for: .seconds(delay))
                    guard let self, let webView else { return }
                    found = await self.read(webView)
                    if found != nil { break }
                }
                self?.onRead(found)
            }
        }

        func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
            Diagnostics.shared.record("PLA flag widget", ok: false, summary: error.localizedDescription)
            onRead(nil)
        }

        func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
            Diagnostics.shared.record("PLA flag widget", ok: false, summary: error.localizedDescription)
            onRead(nil)
        }

        /// Links inside the widget open in Safari rather than inside the card.
        func webView(_ webView: WKWebView, decidePolicyFor navigationAction: WKNavigationAction,
                     decisionHandler: @escaping (WKNavigationActionPolicy) -> Void) {
            if navigationAction.navigationType == .linkActivated, let url = navigationAction.request.url {
                UIApplication.shared.open(url)
                decisionHandler(.cancel)
            } else {
                decisionHandler(.allow)
            }
        }

        private func read(_ webView: WKWebView) async -> FlagColour? {
            let script = """
            (function() {
              document.documentElement.style.background = 'transparent';
              document.body.style.background = 'transparent';
              document.body.style.margin = '0';
              var classes = [], images = [];
              for (const el of document.querySelectorAll('*')) {
                if (typeof el.className === 'string' && el.className) classes.push(el.className);
                if (el.id) classes.push('#' + el.id);
              }
              for (const img of document.querySelectorAll('img, svg use, source')) {
                images.push((img.getAttribute('src') || img.getAttribute('href') || img.getAttribute('srcset') || '')
                            + ' ' + (img.getAttribute('alt') || ''));
              }
              for (const el of document.querySelectorAll('[style*="background"]')) {
                images.push(el.getAttribute('style'));
              }
              return JSON.stringify({ text: document.body.innerText || document.body.textContent || '',
                                      classes: classes.join(' '), images: images.join(' | ') });
            })()
            """
            guard let json = try? await webView.evaluateJavaScript(script) as? String,
                  let page = try? JSONDecoder().decode(WidgetPage.self, from: Data(json.utf8)) else {
                Diagnostics.shared.record("PLA flag widget", ok: false, summary: "Couldn't read the widget page")
                return nil
            }
            let flag = page.flag
            Diagnostics.shared.record(
                "PLA flag widget", ok: flag != nil,
                summary: flag.map { "Read \($0.title) flag from the widget" } ?? "Flag not recognised; showing the widget itself",
                detail: "text: \(page.text.prefix(200)) · classes: \(page.classes.prefix(200)) · images: \(page.images.prefix(200))")
            return flag
        }

        /// Zoom the page so the widget's published width fills the web view.
        static func fit(_ webView: WKWebView) {
            guard webView.bounds.width > 0 else { return }
            let zoom = webView.bounds.width / PLAFlagWidget.size.width
            if abs(webView.pageZoom - zoom) > 0.01 { webView.pageZoom = zoom }
        }
    }
}

/// What the widget page contains, for working out which flag it shows.
struct WidgetPage: Decodable {
    let text: String
    let classes: String
    let images: String

    private static let colours = "black|green|yellow|amber|red"

    /// The flag, only when exactly one colour is named in the place being looked at
    /// (a legend listing all four colours tells us nothing).
    var flag: FlagColour? {
        let c = Self.colours
        return Self.unique(in: text, pattern: "(?:^|\\W)(\(c))\\s+flag")
            ?? Self.unique(in: text, pattern: "flag[^\\n.]{0,30}?\\b(\(c))\\b")
            ?? Self.unique(in: text, pattern: "\\b(\(c))\\b")
            ?? Self.unique(in: classes + " " + images, pattern: "(?:flag|ebb)[-_ /]?(?:icon[-_ ]?)?(\(c))")
            ?? Self.unique(in: classes + " " + images, pattern: "(\(c))[-_ ]?flag")
    }

    private static func unique(in text: String, pattern: String) -> FlagColour? {
        guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else { return nil }
        let range = NSRange(text.startIndex..., in: text)
        let found = Set(regex.matches(in: text, range: range).compactMap { match -> FlagColour? in
            guard let r = Range(match.range(at: 1), in: text) else { return nil }
            let word = text[r].uppercased()
            return FlagColour(rawValue: word == "AMBER" ? "YELLOW" : word)
        })
        return found.count == 1 ? found.first : nil
    }
}

/// The widget in a fixed frame of its published shape, cropped and zoomed to fit.
struct PLAWidgetFrame: View {
    var onRead: @MainActor (FlagColour?) -> Void

    var body: some View {
        let size = PLAFlagWidget.size, crop = PLAFlagWidget.crop
        let visible = CGSize(width: size.width - crop.leading - crop.trailing,
                             height: size.height - crop.top - crop.bottom)
        Color.clear
            .aspectRatio(visible.width / visible.height, contentMode: .fit)
            .overlay(alignment: .topLeading) {
                GeometryReader { geo in
                    let scale = geo.size.width / visible.width
                    PLAFlagWidget(onRead: onRead)
                        .frame(width: size.width * scale, height: size.height * scale)
                        .offset(x: -crop.leading * scale, y: -crop.top * scale)
                }
            }
            .clipped()
    }
}
