import SwiftUI

struct RootView: View {
    @Environment(AppStore.self) private var store
    @Environment(\.scenePhase) private var scenePhase

    var body: some View {
        TabView {
            Tab("Flag", systemImage: "flag.fill") {
                NavigationStack { FlagView() }
            }
            Tab("Weather", systemImage: "cloud.sun.rain.fill") {
                NavigationStack { WeatherView() }
            }
            Tab("Wind", systemImage: "wind") {
                NavigationStack { WindView() }
            }
            Tab("More", systemImage: "ellipsis.circle") {
                NavigationStack { SettingsView() }
            }
        }
        .tabViewStyle(.sidebarAdaptable)       // sidebar on iPad, tab bar on iPhone
        .tabBarMinimizeBehavior(.onScrollDown) // Liquid Glass tab bar shrinks while scrolling
        .onChange(of: scenePhase) { _, phase in
            if phase == .active { Task { await store.refreshAll() } }
        }
    }
}
