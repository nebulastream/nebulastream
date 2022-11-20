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
#ifndef NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTCONFIGURATOR_HPP_
#define NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTCONFIGURATOR_HPP_

#include <Spatial/Index/Location.hpp>
#include <Spatial/Mobility/ReconnectPrediction.hpp>
#include <Util/TimeMeasurement.hpp>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>

#ifdef S2DEF
#include <s2/s1angle.h>
#include <s2/s2point.h>
#endif

namespace NES {
class NesWorker;
using NesWorkerPtr = std::shared_ptr<NesWorker>;

class CoordinatorRPCClient;
using CoordinatorRPCCLientPtr = std::shared_ptr<CoordinatorRPCClient>;

namespace Configurations::Spatial::Mobility::Experimental {
class WorkerMobilityConfiguration;
using WorkerMobilityConfigurationPtr = std::shared_ptr<WorkerMobilityConfiguration>;
}// namespace Configurations::Spatial::Mobility::Experimental

namespace Spatial {
namespace Index::Experimental {
class Location;
using LocationPtr = std::shared_ptr<Location>;
}// namespace Index::Experimental

namespace Mobility::Experimental {
/**
     * @brief runs at worker side and sends periodic updates about location and reconnect predictions to the coordinator. Also
     * allows to trigger a change of the workers parent (reconnect)
     */
class ReconnectConfigurator {
  public:
    /**
     * Constructor
     * @param worker The worker to which this instance belongs
     * @param coordinatorRpcClient This workers rpc client for communicating with the coordinator
     * @param mobilityConfiguration the configuration containing settings related to the operation of the mobile device
     */
    explicit ReconnectConfigurator(
        NesWorker& worker,
        CoordinatorRPCCLientPtr coordinatorRpcClient,
        const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& mobilityConfiguration);

    /**
     * @brief check if the device has moved further than the defined threshold from the last position that was communicated to the coordinator
     * and if so, send the new location and the time it was recorded to the coordinator and safe it as the last transmitted position
     */
    void checkThresholdAndSendLocationUpdate();

    /**
     * @brief keep the coordinator updated about this devices position by periodically calling checkThresholdAndSendLocationUpdate()
     */
    void periodicallySendLocationUpdates();

    /**
     * tell the thread which executes periodicallySendLocationUpdates() to exit the update loop and stop execution
     * @return true if the thread was running, false if no such thread was running
     */
    bool stopPeriodicUpdating();

    /**
     * @brief inform the ReconnectConfigurator about the latest scheduled reconnect. If the supplied reconnect data differs
     * from the previous prediction, it will be sent to the coordinator and also saved as a member of this object
     * @param scheduledReconnect : an optional containing a tuple made up of the id of the expected new parent, the expected
     * Location where the reconnect will happen, and the expected time of the reconnect. Or nullopt in case no prediction
     * exists.
     * @return true if the the supplied prediction differed from the previous prediction. false if the value did not change
     * and therefore no update was sent to the coordinator
     */
    bool updateScheduledReconnect(const std::optional<Mobility::Experimental::ReconnectPrediction>& scheduledReconnect);

    /**
     * @brief change the mobile workers position in the topology by giving it a new parent
     * @param oldParent : the mobile workers old parent
     * @param newParent : the mobile workers new parent
     * @return true if the parents were successfully exchanged
     */
    bool reconnect(uint64_t oldParent, uint64_t newParent);

  private:
    std::atomic<bool> sendUpdates{};
    std::recursive_mutex reconnectConfigMutex;
    NesWorker& worker;
    CoordinatorRPCCLientPtr coordinatorRpcClient;
    std::optional<ReconnectPrediction> lastTransmittedReconnectPrediction;
#ifdef S2DEF
    S2Point lastTransmittedLocation;
    S1Angle locationUpdateThreshold;
#endif
    uint64_t locationUpdateInterval;
    std::shared_ptr<std::thread> sendLocationUpdateThread;
};
using ReconnectConfiguratorPtr = std::shared_ptr<ReconnectConfigurator>;
}// namespace Mobility::Experimental
}// namespace Spatial
}// namespace NES

#endif// NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTCONFIGURATOR_HPP_
