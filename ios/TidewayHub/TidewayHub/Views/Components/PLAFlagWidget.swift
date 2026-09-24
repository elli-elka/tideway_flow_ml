import SwiftUI
import WebKit

/// The PLA's own embeddable Ebb Tide Flag widget: the official current flag.
///
/// The widget page is loaded directly (not inside an iframe) so its real content size
/// can be measured once it has loaded. The frame then takes exactly that shape and
/// the page is zoomed to the available width, so there is no empty space or cropping.
struct PLAFlagWidget: UIViewRepresentable {
    static let url = URL(string: "https://pla.co.uk/pla-api-integration/ebb-tide-widget-embed")!

    /// Measured size of the widget's content (starts at its published 382 x 442).
    @Binding var contentSize: CGSize

    func makeCoordinator() -> Coordinator { Coordinator(contentSize: $contentSize) }

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
        context.coordinator.fit(web)
    }

    @MainActor
    final class Coordinator: NSObject, WKNavigationDelegate {
        private let contentSize: Binding<CGSize>
        private var measured: CGSize?

        init(contentSize: Binding<CGSize>) { self.contentSize = contentSize }

        func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
            // The widget may fill itself in after loading, so measure a few times
            Task { @MainActor [weak self, weak webView] in
                for delay in [0.0, 1.5, 2.5] {
                    try? await Task.sleep(for: .seconds(delay))
                    guard let self, let webView else { return }
                    self.measure(webView)
                }
            }
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

        private func measure(_ webView: WKWebView) {
            let script = """
            (function() {
              document.documentElement.style.background = 'transparent';
              document.body.style.background = 'transparent';
              document.body.style.margin = '0';
              var w = 0, h = 0;
              for (const el of document.body.children) {
                const r = el.getBoundingClientRect();
                w = Math.max(w, r.right); h = Math.max(h, r.bottom);
              }
              if (w < 10 || h < 10) { w = document.body.scrollWidth; h = document.body.scrollHeight; }
              return [Math.ceil(w), Math.ceil(h)];
            })()
            """
            webView.evaluateJavaScript(script) { [weak self] result, _ in
                // CSS pixels, unaffected by pageZoom
                guard let self, let values = result as? [NSNumber], values.count == 2 else { return }
                let size = CGSize(width: values[0].doubleValue, height: values[1].doubleValue)
                guard size.width > 10, size.height > 10 else { return }
                if let old = self.measured, abs(old.width - size.width) < 1, abs(old.height - size.height) < 1 { return }
                self.measured = size
                self.contentSize.wrappedValue = size
                self.fit(webView)
            }
        }

        /// Zoom the page so the widget exactly fills the web view's width.
        func fit(_ webView: WKWebView) {
            guard let measured, measured.width > 0, webView.bounds.width > 0 else { return }
            let zoom = webView.bounds.width / measured.width
            if abs(webView.pageZoom - zoom) > 0.01 { webView.pageZoom = zoom }
        }
    }
}
