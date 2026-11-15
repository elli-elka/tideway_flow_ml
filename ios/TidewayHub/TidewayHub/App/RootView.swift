import SwiftUI

/// Five tabs: the most an iPhone tab bar shows before it hides the rest under "More".
/// The wind map lives inside Weather.
struct RootView: View {
    @Environment(AppStore.self) private var store
    @Environment(\.scenePhase) private var scenePhase

    var body: some View {
        TabView {
            Tab("Flag", systemImage: "flag.fill") {
                NavigationStack { FlagView() }
            }
            Tab("River", systemImage: "water.waves") {
                NavigationStack { RiverView() }
            }
            Tab("Weather", systemImage: "cloud.sun.rain.fill") {
                NavigationStack { WeatherView() }
            }
            Tab("Water", systemImage: "drop.triangle.fill") {
                NavigationStack { WaterView() }
            }
            .badge(store.overflows.filter { $0.status == .discharging }.count)
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
