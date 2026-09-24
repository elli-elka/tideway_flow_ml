import MapKit
import SwiftUI

struct WindView: View {
    @Environment(AppStore.self) private var store
    @AppStorage(SettingsKey.windUnit) private var unitRaw = WindUnit.knots.rawValue
    private var unit: WindUnit { WindUnit(rawValue: unitRaw) ?? .knots }

    @State private var camera: MapCameraPosition = .region(MKCoordinateRegion(
        center: CLLocationCoordinate2D(latitude: 51.4750, longitude: -0.2600),
        span: MKCoordinateSpan(latitudeDelta: 0.055, longitudeDelta: 0.13)))

    var body: some View {
        ScrollView {
            VStack(spacing: 16) {
                CourseMap(camera: $camera, winds: store.courseWind, unit: unit)
                    .frame(height: 340)
                    .clipShape(.rect(cornerRadius: 28))
                    .glassEffect(.regular, in: .rect(cornerRadius: 28))

                if let wind = store.windHere {
                    CrewWindCard(wind: wind, reach: store.reach, stream: store.feed?.richmond?.stream,
                                 usingLocation: store.location.isAuthorized, unit: unit)
                }
                WindyCard(forecast: store.windy, error: store.windyError, unit: unit)
            }
            .padding()
            .frame(maxWidth: 720)
            .frame(maxWidth: .infinity)
        }
        .screenBackground(store.feed?.currentFlag?.flag)
        .navigationTitle("Wind")
        .refreshable {
            await store.refreshCourseWind()
            await store.refreshWeather()
            await store.refreshWindy()
        }
    }
}

private struct CourseMap: View {
    @Binding var camera: MapCameraPosition
    let winds: [Tideway.Waypoint: WindNow]
    let unit: WindUnit

    var body: some View {
        Map(position: $camera) {
            MapPolyline(coordinates: Tideway.course.map(\.coordinate))
                .stroke(.cyan.opacity(0.8), lineWidth: 5)
            ForEach(Tideway.stations) { station in
                Annotation(station.name, coordinate: station.coordinate, anchor: .center) {
                    if let wind = winds[station] {
                        VStack(spacing: 2) {
                            WindArrow(fromDegrees: wind.fromDegrees, size: 16)
                            Text(unit.format(wind.speedKn)).font(.caption2.weight(.bold))
                            if let gust = wind.gustKn {
                                Text("G\(Int(unit.value(fromKnots: gust).rounded()))")
                                    .font(.caption2).foregroundStyle(.orange)
                            }
                        }
                        .padding(6)
                        .glassEffect(.regular, in: .rect(cornerRadius: 12))
                    } else {
                        Circle().fill(.cyan).frame(width: 10, height: 10)
                    }
                }
            }
            UserAnnotation()
        }
        .mapStyle(.standard(elevation: .flat, pointsOfInterest: .excludingAll))
        .mapControls {
            MapUserLocationButton()
            MapCompass()
        }
    }
}

private struct CrewWindCard: View {
    let wind: WindNow
    let reach: Tideway.Reach
    let stream: String?
    let usingLocation: Bool
    let unit: WindUnit

    var body: some View {
        let upstream = CrewWind(wind: wind, heading: reach.upstreamBearing)
        let downstream = CrewWind(wind: wind, heading: (reach.upstreamBearing + 180).truncatingRemainder(dividingBy: 360))
        GlassCard {
            CardHeader(title: usingLocation ? "On your stretch" : "Putney (share location for yours)",
                       systemImage: "figure.rower")
            Text(reach.name).font(.headline)
            HStack(spacing: 10) {
                WindArrow(fromDegrees: wind.fromDegrees)
                Text(windText(wind, unit: unit)).font(.subheadline.weight(.medium))
            }
            Divider().opacity(0.4)
            DirectionRow(title: "Upstream (to Richmond)", systemImage: "arrow.left", crew: upstream, unit: unit)
            DirectionRow(title: "Downstream (to Putney)", systemImage: "arrow.right", crew: downstream, unit: unit)
            if let warning = WindAgainstTide.warning(wind: wind, reach: reach, stream: stream) {
                Label(warning, systemImage: "water.waves")
                    .font(.footnote.weight(.semibold))
                    .foregroundStyle(.orange)
            }
        }
    }
}

private struct DirectionRow: View {
    let title: String
    let systemImage: String
    let crew: CrewWind
    let unit: WindUnit

    var body: some View {
        HStack {
            Label(title, systemImage: systemImage).font(.subheadline)
            Spacer()
            Text(description).font(.subheadline.weight(.semibold))
        }
    }

    private var description: String {
        var parts: [String] = []
        if abs(crew.head) >= 1 {
            parts.append("\(Int(unit.value(fromKnots: abs(crew.head)).rounded())) \(crew.head > 0 ? "head" : "tail")")
        }
        if abs(crew.cross) >= 1 {
            parts.append("\(Int(unit.value(fromKnots: abs(crew.cross)).rounded())) cross \(crew.cross > 0 ? "R" : "L")")
        }
        return parts.isEmpty ? "Calm" : parts.joined(separator: " · ")
    }
}

private struct WindyCard: View {
    let forecast: [WindNow]
    let error: String?
    let unit: WindUnit

    var body: some View {
        GlassCard {
            CardHeader(title: "Windy (ICON-EU)", systemImage: "wind.circle")
            if let error {
                Text(error).font(.footnote).foregroundStyle(.orange)
            } else if forecast.isEmpty {
                Text("Add a Windy Point Forecast API key in More to compare with Windy's model here.")
                    .font(.footnote).foregroundStyle(.secondary)
            } else {
                let upcoming = forecast.filter { $0.time > Date().addingTimeInterval(-3 * 3600) }.prefix(8)
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 14) {
                        ForEach(Array(upcoming), id: \.self) { w in
                            VStack(spacing: 4) {
                                Text(w.time.formatted(.dateTime.hour())).font(.caption).foregroundStyle(.secondary)
                                WindArrow(fromDegrees: w.fromDegrees, size: 13)
                                Text("\(Int(unit.value(fromKnots: w.speedKn).rounded()))").font(.subheadline.weight(.semibold))
                                if let gust = w.gustKn {
                                    Text("G\(Int(unit.value(fromKnots: gust).rounded()))").font(.caption2).foregroundStyle(.orange)
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
