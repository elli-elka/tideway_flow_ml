import SwiftUI
import WebKit

/// The PLA's own embeddable Ebb Tide Flag widget: the official current flag.
struct PLAFlagWidget: UIViewRepresentable {
    static let url = URL(string: "https://pla.co.uk/pla-api-integration/ebb-tide-widget-embed")!
    /// The widget is designed at 382 x 442 points.
    static let aspectRatio: CGFloat = 382 / 442

    func makeUIView(context: Context) -> WKWebView {
        let web = WKWebView(frame: .zero, configuration: WKWebViewConfiguration())
        web.isOpaque = false
        web.backgroundColor = .clear
        web.scrollView.backgroundColor = .clear
        web.scrollView.isScrollEnabled = false
        web.scrollView.bounces = false
        // Wrap the iframe in a page whose viewport matches the widget, so WebKit
        // scales it to whatever width the card has
        let html = """
        <html><head>
        <meta name="viewport" content="width=382, initial-scale=1">
        <style>html,body{margin:0;padding:0;background:transparent;overflow:hidden}</style>
        </head><body>
        <iframe src="\(Self.url.absoluteString)" width="382" height="442" frameborder="0" scrolling="no"></iframe>
        </body></html>
        """
        web.loadHTMLString(html, baseURL: URL(string: "https://pla.co.uk"))
        return web
    }

    func updateUIView(_ web: WKWebView, context: Context) {}
}
