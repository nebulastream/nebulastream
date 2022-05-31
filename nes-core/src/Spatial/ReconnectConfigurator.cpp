/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <Spatial/ReconnectConfigurator.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <Components/NesWorker.hpp>
#include <Common/Location.hpp>
#include <utility>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <s2/s2point.h>
#include <s2/s1angle.h>
#include <s2/s2latlng.h>
#include <s2/s2earth.h>

namespace NES {
NES::Spatial::Mobility::Experimental::ReconnectConfigurator::ReconnectConfigurator(
        NesWorker& worker,
        CoordinatorRPCClientPtr coordinatorRpcClient,
        const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& mobilityConfiguration) : worker(worker), coordinatorRpcClient(std::move(coordinatorRpcClient)) {
        locationUpdateThreshold = S2Earth::MetersToAngle(mobilityConfiguration->sendDevicePositionUpdateThreshold);
        locationUpdateInterval = mobilityConfiguration->sendLocationUpdateInterval;
        if (mobilityConfiguration->pushDeviceLocationUpdates) {
           sendLocationUpdateThread = std::make_shared<std::thread>(&ReconnectConfigurator::periodicallyUpdateLocation, this);
        }
};
bool NES::Spatial::Mobility::Experimental::ReconnectConfigurator::update(
    const std::optional<std::tuple<uint64_t, Index::Experimental::LocationPtr, Timestamp>>& scheduledReconnect) {

    if (scheduledReconnect.has_value()) {
        uint64_t reconnectId = get<0>(scheduledReconnect.value());
        Timestamp timestamp = get<2>(scheduledReconnect.value());
        if (!lastTransmittedReconnectPrediction.has_value()) {
            NES_DEBUG("transmitting predicted reconnect point. previous prediction did not exist")
            coordinatorRpcClient->sendReconnectPrediction(worker.getWorkerId(), scheduledReconnect.value());
        } else if (reconnectId != get<0>(lastTransmittedReconnectPrediction.value()) || timestamp != get<2>(lastTransmittedReconnectPrediction.value())) {
            NES_DEBUG("transmitting predicted reconnect point. current prediction differs from previous prediction")
            coordinatorRpcClient->sendReconnectPrediction(worker.getWorkerId(), scheduledReconnect.value());
        }
    } else if (lastTransmittedReconnectPrediction.has_value()) {
        NES_DEBUG("no reconnect point found after recalculation, telling coordinator to discard old reconnect")
        coordinatorRpcClient->sendReconnectPrediction(worker.getWorkerId(), std::tuple<uint64_t, Index::Experimental::LocationPtr, Timestamp>(0, {}, 0));
    }
    lastTransmittedReconnectPrediction = scheduledReconnect;
    return false;
}
bool NES::Spatial::Mobility::Experimental::ReconnectConfigurator::reconnect(uint64_t oldParent, uint64_t newParent) {
    return worker.replaceParent(oldParent, newParent);
}

void NES::Spatial::Mobility::Experimental::ReconnectConfigurator::sendPeriodicLocationUpdate() {
    auto currentLocationTuple = worker.getLocationProvider()->getCurrentLocation();
    auto currentLocation = currentLocationTuple.first;
    S2Point currentPoint(S2LatLng::FromDegrees(currentLocation->getLatitude(), currentLocation->getLongitude()));
    if (S1Angle(currentPoint, lastTransmittedLocation) > locationUpdateThreshold) {
        NES_DEBUG("device has moved further then threshold, sending location")
        coordinatorRpcClient->sendLocationUpdate(worker.getWorkerId(), currentLocationTuple);
        lastTransmittedLocation = currentPoint;
    } else {
        NES_DEBUG("device has not moved further than threshold, location will not be transmitted")
    }
}

void NES::Spatial::Mobility::Experimental::ReconnectConfigurator::periodicallyUpdateLocation() {
    auto currentLocationTuple = worker.getLocationProvider()->getCurrentLocation();
    auto currentLocation = currentLocationTuple.first;
    S2Point currentPoint(S2LatLng::FromDegrees(currentLocation->getLatitude(), currentLocation->getLongitude()));
    NES_DEBUG("transmitting initial location")
    coordinatorRpcClient->sendLocationUpdate(worker.getWorkerId(), currentLocationTuple);
    lastTransmittedLocation = currentPoint;
    while (true) {
        sendPeriodicLocationUpdate();
        std::this_thread::sleep_for(std::chrono::milliseconds(locationUpdateInterval));
    }
}

}