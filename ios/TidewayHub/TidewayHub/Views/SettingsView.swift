import SwiftUI

struct SettingsView: View {
    @Environment(AppStore.self) private var store
    @AppStorage(SettingsKey.windUnit) private var unitRaw = WindUnit.knots.rawValue
    @AppStorage(SettingsKey.windyAPIKey) private var windyKey = ""
    @AppStorage(SettingsKey.feedURL) private var feedURL = FeedService.defaultURL.absoluteString

    var body: some View {
        Form {
            Section("Units") {
                Picker("Wind speed", selection: $unitRaw) {
                    ForEach(WindUnit.allCases) { Text($0.rawValue).tag($0.rawValue) }
                }
                .pickerStyle(.segmented)
            }

            Section {
                SecureField("Windy Point Forecast API key", text: $windyKey)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled()
                Link("Get a key at api.windy.com", destination: URL(string: "https://api.windy.com/keys")!)
            } header: {
                Text("Windy (optional)")
            } footer: {
                Text("Windy's free testing keys return shuffled data; real values need a paid Point Forecast key.")
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
                Text("Set at 6am and 6pm from the lowest Richmond tide reading of the previous 12 hours.")
            }

            Section {
                LabeledContent("Feed updated", value: store.feed?.generatedAt.ago ?? "–")
                LabeledContent("Last refresh", value: store.lastRefresh?.ago ?? "–")
                if let model = store.feed?.predictions?.modelVersion {
                    LabeledContent("Model", value: model)
                }
                TextField("Feed URL", text: $feedURL)
                    .font(.footnote)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled()
                Button("Refresh everything") { Task { await store.refreshAll() } }
            } header: {
                Text("Data")
            } footer: {
                Text("Flags, predictions and levels: Tideway Flow ML pipeline (EA and PLA data, OGL). Weather: Open-Meteo (CC BY 4.0), Met Office UKMO model. Measured wind: aviationweather.gov. Unofficial app; always check the official flag.")
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
