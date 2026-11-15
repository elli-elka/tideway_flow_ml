import SwiftUI

struct SettingsView: View {
    @Environment(AppStore.self) private var store
    @AppStorage(SettingsKey.windUnit) private var unitRaw = WindUnit.knots.rawValue
    @AppStorage(SettingsKey.showDiagnostics) private var showDiagnostics = false
    @AppStorage(SettingsKey.flagAlerts) private var flagAlertsRaw = AlertService.FlagAlerts.off.rawValue
    @AppStorage(SettingsKey.sewageAlerts) private var sewageAlerts = false
    @AppStorage(SettingsKey.clubAlerts) private var clubAlerts = false
    @AppStorage(SettingsKey.homeClub) private var homeClub = ""

    var body: some View {
        Form {
            Section("Units") {
                Picker("Wind speed", selection: $unitRaw) {
                    ForEach(WindUnit.allCases) { Text($0.rawValue).tag($0.rawValue) }
                }
                .pickerStyle(.segmented)
            }

            Section {
                Picker("Flag changes", selection: $flagAlertsRaw) {
                    ForEach(AlertService.FlagAlerts.allCases) { Text($0.rawValue).tag($0.rawValue) }
                }
                Toggle("Sewage discharges nearby", isOn: $sewageAlerts)
                    .disabled(!store.hasThamesWaterKey)
                Toggle("Arriving at my club", isOn: $clubAlerts)
                if clubAlerts {
                    Picker("Home club", selection: $homeClub) {
                        Text("Choose…").tag("")
                        ForEach(Club.tideway) { Text($0.name).tag($0.name) }
                        Text("My location (set below)").tag(Club.custom)
                    }
                    Button("Set my club to where I am now", systemImage: "location.fill") { useCurrentLocation() }
                        .disabled(store.location.location == nil)
                }
            } header: {
                Text("Alerts")
            } footer: {
                Text(alertsFooter)
            }
            .onChange(of: flagAlertsRaw) { _, value in
                if value != AlertService.FlagAlerts.off.rawValue { enableAlerts() }
            }
            .onChange(of: sewageAlerts) { _, on in if on { enableAlerts() } }
            .onChange(of: clubAlerts) { _, _ in updateGeofence() }
            .onChange(of: homeClub) { _, _ in updateGeofence() }

            Section {
                FlagLegendRow(colour: .red)
                FlagLegendRow(colour: .yellow)
                FlagLegendRow(colour: .green)
                FlagLegendRow(colour: .black)
                Link("Official PLA Ebb Tide Flag", destination: store.feed?.officialFlagUrl
                     ?? URL(string: "https://pla.co.uk/ebb-tide-flag-warning")!)
                Link("Tideway Code", destination: URL(string: "https://pla.co.uk/tideway-code")!)
            } header: {
                Text("Ebb Tide Flag")
            } footer: {
                Text("Set at 06:00 and 18:00 from the lowest Richmond tide reading of the previous 12 hours.")
            }

            Section {
                LabeledContent("Feed updated", value: store.feed?.generatedAt.ago ?? "–")
                LabeledContent("Last refresh", value: store.lastRefresh?.ago ?? "–")
                if let model = store.feed?.predictions?.modelVersion {
                    LabeledContent("Model", value: model)
                }
                Button("Refresh everything") { Task { await store.refreshAll() } }
            } header: {
                Text("Data")
            } footer: {
                Text("Flags, predictions and levels: Tideway Flow ML pipeline (EA and PLA data, OGL). Weather: Open-Meteo (CC BY 4.0), Met Office UKMO model. Measured wind: aviationweather.gov. Kingston flow: Environment Agency (OGL). Storm overflows: Thames Water Open Data. Unofficial app; always check the official flag.")
            }

            Section {
                Toggle("Show diagnostics", isOn: $showDiagnostics)
            } header: {
                Text("Advanced")
            }

            if showDiagnostics {
            Section {
                ForEach(Diagnostics.shared.sorted) { entry in
                    VStack(alignment: .leading, spacing: 4) {
                        HStack {
                            Circle().fill(entry.ok ? .green : .orange).frame(width: 8, height: 8)
                            Text(entry.source).font(.subheadline.weight(.semibold))
                            Spacer()
                            Text(UKTime.hm(entry.time)).font(.caption).foregroundStyle(.secondary)
                        }
                        Text(entry.summary).font(.caption)
                        if let detail = entry.detail {
                            Text(detail)
                                .font(.system(size: 10, design: .monospaced))
                                .foregroundStyle(.secondary)
                                .lineLimit(4)
                        }
                    }
                    .textSelection(.enabled)
                }
                if Diagnostics.shared.entries.isEmpty {
                    Text("Nothing loaded yet. Pull to refresh on any tab.").font(.caption)
                }
            } header: {
                Text("Diagnostics")
            } footer: {
                Text("What each data source last returned. Screenshot this if something looks wrong.")
            }
            }
        }
        .scrollContentBackground(.hidden)
        .screenBackground(store.displayFlag)
        .navigationTitle("More")
    }
}

extension SettingsView {
    private var alertsFooter: String {
        var text = "Flag alerts are worked out on your phone from the Richmond gauge just after 06:00 and 18:00. iOS decides exactly when the app can check in the background, so they can be a little late, and they stop if you force-quit the app or turn off Background App Refresh."
        if !store.hasThamesWaterKey { text += " Sewage alerts need a Thames Water key (see the Water tab)." }
        text += " Arriving at your club shows the flag, forecast and next tide on your lock screen; it needs location access set to Always."
        return text
    }

    private func enableAlerts() {
        Task {
            await AlertService.shared.requestPermission()
            AlertService.shared.scheduleRefresh()
        }
    }

    private func updateGeofence() {
        guard clubAlerts else {
            store.location.monitorHomeClub(nil)
            return
        }
        enableAlerts()
        store.location.monitorHomeClub(Club.saved)
    }

    private func useCurrentLocation() {
        guard let here = store.location.location else { return }
        UserDefaults.standard.set(here.coordinate.latitude, forKey: SettingsKey.homeClubLatitude)
        UserDefaults.standard.set(here.coordinate.longitude, forKey: SettingsKey.homeClubLongitude)
        if homeClub == Club.custom { updateGeofence() } else { homeClub = Club.custom }
    }
}

private struct FlagLegendRow: View {
    let colour: FlagColour

    var body: some View {
        HStack(spacing: 12) {
            FlagGlyph(colour: colour, size: 17)
            VStack(alignment: .leading) {
                Text(colour.title).font(.subheadline.weight(.semibold))
                Text(colour.summary).font(.caption).foregroundStyle(.secondary)
            }
            Spacer()
            Text(colour.range).font(.caption).foregroundStyle(.secondary)
        }
    }
}
