import SwiftUI

struct SettingsView: View {
    @Environment(AppStore.self) private var store
    @AppStorage(SettingsKey.windUnit) private var unitRaw = WindUnit.knots.rawValue
    @AppStorage(SettingsKey.showDiagnostics) private var showDiagnostics = false

    var body: some View {
        Form {
            Section("Units") {
                Picker("Wind speed", selection: $unitRaw) {
                    ForEach(WindUnit.allCases) { Text($0.rawValue).tag($0.rawValue) }
                }
                .pickerStyle(.segmented)
            }

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
                Text("Flags, predictions and levels: Tideway Flow ML pipeline (EA and PLA data, OGL). Weather: Open-Meteo (CC BY 4.0), Met Office UKMO model. Measured wind: aviationweather.gov. Unofficial app; always check the official flag.")
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
        .screenBackground(store.feed?.currentFlag?.flag)
        .navigationTitle("More")
    }
}

private struct FlagLegendRow: View {
    let colour: FlagColour

    var body: some View {
        HStack(spacing: 12) {
            Image(systemName: "flag.fill").foregroundStyle(colour.color)
            VStack(alignment: .leading) {
                Text(colour.title).font(.subheadline.weight(.semibold))
                Text(colour.summary).font(.caption).foregroundStyle(.secondary)
            }
            Spacer()
            Text(colour.range).font(.caption).foregroundStyle(.secondary)
        }
    }
}
