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
#ifndef NES_RECONNECTCONFIGURATOR_HPP
#define NES_RECONNECTCONFIGURATOR_HPP

#include <memory>
#include <optional>
#include <Util/TimeMeasurement.hpp>
#include <s2/s2point.h>
#include <s2/s1angle.h>
#include <thread>

namespace NES {
class NesWorker;
using NesWorkerPtr = std::shared_ptr<NesWorker>;

class CoordinatorRPCClient;
using CoordinatorRPCCLientPtr = std::shared_ptr<CoordinatorRPCClient>;

namespace Configurations::Spatial::Mobility::Experimental {
class WorkerMobilityConfiguration;
using WorkerMobilityConfigurationPtr = std::shared_ptr<WorkerMobilityConfiguration>;
}

namespace Spatial {
namespace Index::Experimental {
class Location;
using LocationPtr = std::shared_ptr<Location>;
}

namespace Mobility::Experimental {
    class ReconnectConfigurator {
      public:
        explicit ReconnectConfigurator(NesWorker& worker,
            CoordinatorRPCCLientPtr coordinatorRpcClient, const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& mobilityConfiguration);

        void sendPeriodicLocationUpdate();

        void periodicallyUpdateLocation();

        /**
         *
         * @param scheduledReconnect
         * @return
         */
        bool update(const std::optional<std::tuple<uint64_t, Index::Experimental::LocationPtr, Timestamp>>& scheduledReconnect);
        bool reconnect(uint64_t oldParent, uint64_t newParent);
      private:
        NesWorker& worker;
        CoordinatorRPCCLientPtr coordinatorRpcClient;
        std::optional<std::tuple<uint64_t, Index::Experimental::LocationPtr, Timestamp>> lastTransmittedReconnectPrediction;
        S2Point lastTransmittedLocation;
        S1Angle locationUpdateThreshold;
        uint64_t locationUpdateInterval;
        std::shared_ptr<std::thread> sendLocationUpdateThread;
    };
    using ReconnectConfiguratorPtr = std::shared_ptr<ReconnectConfigurator>;
}
}
}


#endif//NES_RECONNECTCONFIGURATOR_HPP
