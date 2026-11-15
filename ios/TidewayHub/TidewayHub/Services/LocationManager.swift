import CoreLocation
import Observation

@MainActor @Observable
final class LocationManager: NSObject, CLLocationManagerDelegate {
    var location: CLLocation?
    var authorization: CLAuthorizationStatus

    @ObservationIgnored private let manager: CLLocationManager

    override init() {
        let manager = CLLocationManager()
        self.manager = manager
        self.authorization = manager.authorizationStatus
        super.init()
        manager.delegate = self
        manager.desiredAccuracy = kCLLocationAccuracyHundredMeters
    }

    var isAuthorized: Bool {
        authorization == .authorizedWhenInUse || authorization == .authorizedAlways
    }

    func request() {
        if manager.authorizationStatus == .notDetermined {
            manager.requestWhenInUseAuthorization()
        } else if isAuthorized {
            manager.requestLocation()
        }
    }

    nonisolated func locationManager(_ manager: CLLocationManager, didUpdateLocations locations: [CLLocation]) {
        guard let latest = locations.last else { return }
        Task { @MainActor in self.location = latest }
    }

    nonisolated func locationManager(_ manager: CLLocationManager, didFailWithError error: Error) {}

    // MARK: Home club geofence

    nonisolated static let clubRegionID = "homeClub"

    /// Watch for arriving at `club` (or stop watching if nil). iOS does the watching,
    /// even when the app isn't running, and wakes the app on arrival. Needs "Always"
    /// location permission; iOS asks for it the first time.
    func monitorHomeClub(_ club: Club?) {
        for region in manager.monitoredRegions where region.identifier == Self.clubRegionID {
            manager.stopMonitoring(for: region)
        }
        guard let club else { return }
        guard CLLocationManager.isMonitoringAvailable(for: CLCircularRegion.self) else {
            Diagnostics.shared.record("Home club", ok: false, summary: "Geofencing isn't available on this device")
            return
        }
        if manager.authorizationStatus != .authorizedAlways { manager.requestAlwaysAuthorization() }
        let region = CLCircularRegion(center: club.coordinate, radius: 300, identifier: Self.clubRegionID)
        region.notifyOnEntry = true
        region.notifyOnExit = false
        manager.startMonitoring(for: region)
        Diagnostics.shared.record("Home club", ok: true, summary: "Watching for arrival at \(club.name)")
    }

    nonisolated func locationManager(_ manager: CLLocationManager, didEnterRegion region: CLRegion) {
        guard region.identifier == Self.clubRegionID else { return }
        Task { @MainActor in
            guard let club = Club.saved else { return }
            await AlertService.shared.arrivedAtClub(club)
        }
    }

    nonisolated func locationManager(_ manager: CLLocationManager, monitoringDidFailFor region: CLRegion?,
                                     withError error: Error) {
        let message = error.localizedDescription
        Task { @MainActor in Diagnostics.shared.record("Home club", ok: false, summary: message) }
    }

    nonisolated func locationManagerDidChangeAuthorization(_ manager: CLLocationManager) {
        let status = manager.authorizationStatus
        Task { @MainActor in
            self.authorization = status
            if self.isAuthorized { self.manager.requestLocation() }
        }
    }
}
