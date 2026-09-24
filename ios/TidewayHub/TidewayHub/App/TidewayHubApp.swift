import SwiftUI

@main
struct TidewayHubApp: App {
    @State private var store = AppStore()

    var body: some Scene {
        WindowGroup {
            RootView()
                .environment(store)
                .task {
                    await store.refreshAll()
                    await store.autoRefresh()
                }
        }
    }
}
