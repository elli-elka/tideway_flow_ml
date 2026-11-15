import SwiftUI

@main
struct TidewayHubApp: App {
    @State private var store = AppStore()

    var body: some Scene {
        WindowGroup {
            RootView()
                .environment(store)
                .task {
                    if UserDefaults.standard.bool(forKey: SettingsKey.clubAlerts) {
                        store.location.monitorHomeClub(Club.saved)
                    }
                    AlertService.shared.scheduleRefresh()
                    await store.refreshAll()
                    await store.autoRefresh()
                }
        }
        // iOS wakes the app now and then to check for flag changes and sewage
        // discharges (see AlertService)
        .backgroundTask(.appRefresh(AlertService.refreshTaskID)) {
            let offset = FeedService().loadOffline().feed?.richmondCdOffset ?? 0.705
            await AlertService.shared.runChecks(offset: offset)
        }
    }
}
