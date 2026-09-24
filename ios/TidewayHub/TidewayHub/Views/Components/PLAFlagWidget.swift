import SwiftUI
import WebKit

/// The PLA's own embeddable Ebb Tide Flag widget: the official current flag.
///
/// The PLA page is laid out at a fixed 382 x 442 points. Rather than relying on the
/// viewport (which can't shrink it and left it hanging off to one side), the page is
/// wrapped in a box that is scaled to exactly the web view's width, so the widget
/// fills its frame edge to edge and the card centres it.
struct PLAFlagWidget: UIViewRepresentable {
    static let url = URL(string: "https://pla.co.uk/pla-api-integration/ebb-tide-widget-embed")!

    /// The size the PLA page is designed at.
    static let designSize = CGSize(width: 382, height: 442)

    /// Dead space to trim off the PLA page's own edges. Nudge a few points at a time
    /// until it looks centred, e.g. EdgeInsets(top: 8, leading: 8, bottom: 8, trailing: 8).
    /// (Safari's Web Inspector on the widget URL shows where its content sits.)
    static let crop = EdgeInsets(top: 0, leading: 0, bottom: 1, trailing: 0)

    static var visibleSize: CGSize {
        CGSize(width: designSize.width - crop.leading - crop.trailing,
               height: designSize.height - crop.top - crop.bottom)
    }

    static var aspectRatio: CGFloat { visibleSize.width / visibleSize.height }

    func makeUIView(context: Context) -> WKWebView {
        let web = WKWebView(frame: .zero, configuration: WKWebViewConfiguration())
        web.isOpaque = false
        web.backgroundColor = .clear
        web.scrollView.backgroundColor = .clear
        web.scrollView.isScrollEnabled = false
        web.scrollView.bounces = false

        let d = Self.designSize, v = Self.visibleSize, c = Self.crop
        let html = """
        <html><head>
        <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
        <style>
          html,body{margin:0;padding:0;background:transparent;overflow:hidden}
          #wrap{width:\(v.width)px;height:\(v.height)px;overflow:hidden;transform-origin:top left}
          iframe{display:block;border:0;width:\(d.width)px;height:\(d.height)px;
                 margin-left:-\(c.leading)px;margin-top:-\(c.top)px}
        </style>
        </head><body>
        <div id="wrap"><iframe src="\(Self.url.absoluteString)" scrolling="no"></iframe></div>
        <script>
          function fit() {
            document.getElementById('wrap').style.transform =
              'scale(' + (window.innerWidth / \(v.width)) + ')';
          }
          fit();
          window.addEventListener('resize', fit);
        </script>
        </body></html>
        """
        web.loadHTMLString(html, baseURL: URL(string: "https://pla.co.uk"))
        return web
    }

    func updateUIView(_ web: WKWebView, context: Context) {}
}
