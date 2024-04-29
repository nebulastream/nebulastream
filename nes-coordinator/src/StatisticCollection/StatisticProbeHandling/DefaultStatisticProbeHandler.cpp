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

#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <StatisticCollection/StatisticProbeHandling/DefaultStatisticProbeHandler.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Statistic {

AbstractStatisticProbeHandlerPtr DefaultStatisticProbeHandler::create() {
    return std::make_shared<DefaultStatisticProbeHandler>();
}

std::vector<StatisticProbeRequestGRPC> DefaultStatisticProbeHandler::getProbeRequests(const StatisticRegistry&,
                                                                                      const StatisticProbeRequest& probeRequest,
                                                                                      const Topology& topology) {
    NES_DEBUG("Creating probe requests for probe request");
    std::vector<StatisticProbeRequestGRPC> allProbeRequests;
    const auto allWorkerIds = topology.getAllRegisteredNodeIds();
    for (const auto& workerId : allWorkerIds) {
        const auto topologyNode = topology.getCopyOfTopologyNodeWithId(workerId);
        const auto gRPCAddress = topologyNode->getIpAddress() + ":" + std::to_string(topologyNode->getGrpcPort());
        allProbeRequests.emplace_back(probeRequest, gRPCAddress);
    }

    return allProbeRequests;
}
}// namespace NES::Statistic
