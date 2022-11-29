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
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Spatial/Index/Location.hpp>
#include <Spatial/Index/Waypoint.hpp>
#include <Spatial/Mobility/LocationProviderCSV.hpp>
#include <Spatial/Mobility/ReconnectConfigurator.hpp>
#include <Spatial/Mobility/ReconnectPrediction.hpp>
#include <utility>
#ifdef S2DEF
#include <Util/Experimental/S2Utilities.hpp>
#include <s2/s1angle.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#include <s2/s2point.h>
#endif

namespace NES {
NES::Spatial::Mobility::Experimental::ReconnectConfigurator::ReconnectConfigurator(
    NesWorker& worker,
    CoordinatorRPCClientPtr coordinatorRpcClient,
    const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& mobilityConfiguration)
    : worker(worker), coordinatorRpcClient(std::move(coordinatorRpcClient)) {
    locationUpdateInterval = mobilityConfiguration->sendLocationUpdateInterval;
    sendUpdates = false;
#ifdef S2DEF
    locationUpdateThreshold = S2Earth::MetersToAngle(mobilityConfiguration->sendDevicePositionUpdateThreshold);
    if (mobilityConfiguration->pushDeviceLocationUpdates) {
        sendUpdates = true;
        sendLocationUpdateThread = std::make_shared<std::thread>(&ReconnectConfigurator::periodicallySendLocationUpdates, this);
    }
#endif
};
bool NES::Spatial::Mobility::Experimental::ReconnectConfigurator::updateScheduledReconnect(
    const std::optional<NES::Spatial::Mobility::Experimental::ReconnectPrediction>& scheduledReconnect) {
    bool predictionChanged = false;
    if (scheduledReconnect.has_value()) {
        // the new value represents a valid prediction
        uint64_t reconnectId = scheduledReconnect.value().expectedNewParentId;
        Timestamp timestamp = scheduledReconnect.value().expectedTime;
        std::unique_lock lock(reconnectConfigMutex);
        if (!lastTransmittedReconnectPrediction.has_value()) {
            // previously there was no prediction. we now inform the coordinator that a prediction exists
            NES_DEBUG("transmitting predicted reconnect point. previous prediction did not exist")
            coordinatorRpcClient->sendReconnectPrediction(worker.getWorkerId(), scheduledReconnect.value());
            predictionChanged = true;
        } else if (reconnectId != lastTransmittedReconnectPrediction.value().expectedNewParentId
                   || timestamp != lastTransmittedReconnectPrediction.value().expectedTime) {
            // there was a previous prediction but its values differ from the current one. Inform coordinator about the new prediciton
            NES_DEBUG("transmitting predicted reconnect point. current prediction differs from previous prediction")
            coordinatorRpcClient->sendReconnectPrediction(worker.getWorkerId(), scheduledReconnect.value());
            lastTransmittedReconnectPrediction = scheduledReconnect;
            predictionChanged = true;
        }
    } else if (lastTransmittedReconnectPrediction.has_value()) {
        // a previous trajectory led to the calculation of a prediction. But there is no prediction (yet) for the current trajectory
        // inform coordinator, that the old prediction is not valid anymore
        NES_DEBUG("no reconnect point found after recalculation, telling coordinator to discard old reconnect")
        coordinatorRpcClient->sendReconnectPrediction(worker.getWorkerId(),
                                                      NES::Spatial::Mobility::Experimental::ReconnectPrediction{0, 0});
        predictionChanged = true;
    }
    lastTransmittedReconnectPrediction = scheduledReconnect;
    return predictionChanged;
}

bool NES::Spatial::Mobility::Experimental::ReconnectConfigurator::reconnect(uint64_t oldParent, uint64_t newParent) {
    worker.getNodeEngine()->bufferAllData();
    //todo #3027: wait until all upstream operators have received data which has not been buffered
    //todo #3027: trigger replacement and migration of operators
    bool success = worker.replaceParent(oldParent, newParent);
    worker.getNodeEngine()->stopBufferingAllData();
    return success;
}

#ifdef S2DEF
void NES::Spatial::Mobility::Experimental::ReconnectConfigurator::checkThresholdAndSendLocationUpdate() {
    auto locProvider = worker.getLocationProvider();
    if (locProvider) {
        //get the devices current location
        auto currentWaypoint = locProvider->getCurrentWaypoint();
        auto currentPoint = NES::Spatial::Util::S2Utilities::locationToS2Point(*currentWaypoint->getLocation());

        std::unique_lock lock(reconnectConfigMutex);
        //check if we moved further than the threshold. if so, tell the coordinator about the devices new position
        if (S1Angle(currentPoint, lastTransmittedLocation) > locationUpdateThreshold) {
            NES_DEBUG("device has moved further then threshold, sending location")
            coordinatorRpcClient->sendLocationUpdate(worker.getWorkerId(), currentWaypoint);
            lastTransmittedLocation = currentPoint;
        } else {
            NES_DEBUG("device has not moved further than threshold, location will not be transmitted")
        }
    }
}

void NES::Spatial::Mobility::Experimental::ReconnectConfigurator::periodicallySendLocationUpdates() {
    //get the devices current location
    auto currentWaypoint = worker.getLocationProvider()->getCurrentWaypoint();
    auto currentPoint = NES::Spatial::Util::S2Utilities::locationToS2Point(*currentWaypoint->getLocation());

    NES_DEBUG("transmitting initial location")
    coordinatorRpcClient->sendLocationUpdate(worker.getWorkerId(), currentWaypoint);
    std::unique_lock lock(reconnectConfigMutex);
    lastTransmittedLocation = currentPoint;
    lock.unlock();

    //start periodically pulling location updates and inform coordinator about location changes
    while (sendUpdates) {
        checkThresholdAndSendLocationUpdate();
        std::this_thread::sleep_for(std::chrono::milliseconds(locationUpdateInterval));
    }
}
#endif

bool NES::Spatial::Mobility::Experimental::ReconnectConfigurator::stopPeriodicUpdating() {
    if (!sendUpdates) {
        return false;
    }
    sendUpdates = false;
    return true;
}

}// namespace NES