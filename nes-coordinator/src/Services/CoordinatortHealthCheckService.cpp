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
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Services/CoordinatorHealthCheckService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>

namespace NES {

CoordinatorHealthCheckService::CoordinatorHealthCheckService(TopologyPtr topology,
                                                             std::string healthServiceName,
                                                             Configurations::CoordinatorConfigurationPtr coordinatorConfiguration)
    : topology(topology), workerRPCClient(WorkerRPCClient::create()), coordinatorConfiguration(coordinatorConfiguration) {
    id = 9999;
    this->healthServiceName = healthServiceName;
}

void CoordinatorHealthCheckService::startHealthCheck() {
    NES_INFO("CoordinatorHealthCheckService::startHealthCheck");
    isRunning = true;
    NES_DEBUG("start health checking on coordinator");
    healthCheckingThread = std::make_shared<std::thread>(([this]() {
        setThreadName("nesHealth");
        auto waitTime = std::chrono::seconds(this->coordinatorConfiguration->coordinatorHealthCheckWaitTime.getValue());
        while (isRunning) {
            for (auto node : topologyIdToRPCAddressMap.lock_table()) {
                std::string destAddress = node.second;
                //check health
                NES_INFO("NesCoordinator::healthCheck: checking node= {}", destAddress);
                auto res = workerRPCClient->checkHealth(destAddress, healthServiceName);
                if (res) {
                    NES_TRACE("NesCoordinator::healthCheck: node={} is alive", destAddress);
                    if (inactiveWorkers.contains(node.first)) {
                        inactiveWorkers.erase(node.first);
                    }
                } else {
                    NES_WARNING("NesCoordinator::healthCheck: node={} went dead so we remove it", destAddress);
                    const auto& rootWorkerNodeIds = topology->getRootWorkerNodeIds();
                    auto found = std::find(rootWorkerNodeIds.begin(), rootWorkerNodeIds.end(), node.first);
                    if (found != rootWorkerNodeIds.end()) {
                        NES_WARNING("The failing node is the root node so we cannot delete it");
                        shutdownRPC->set_value(true);
                        return;
                    } else {
                        auto ret = topology->unregisterWorker(node.first);
                        inactiveWorkers.insert(node.first);
                        if (ret) {
                            NES_TRACE("NesCoordinator::healthCheck: remove node={} successfully", destAddress);
                        } else {
                            NES_WARNING("Node went offline but could not be removed from topology");
                        }
                        //TODO Create issue for remove and child
                        //TODO add remove from source catalog
                    }
                }
            }
            {
                std::unique_lock<std::mutex> lk(cvMutex);
                cv.wait_for(lk, waitTime, [this] {
                    return isRunning == false;
                });
            }
        }
        shutdownRPC->set_value(true);
        NES_DEBUG("NesCoordinator: stop health checking");
    }));
}

void CoordinatorHealthCheckService::informWorkerAboutNeighbors(WorkerId targetWorkerId, std::string targetAddress, std::vector<std::pair<WorkerId, std::string>> neighborInfo) {
    NES_DEBUG("informWorkerAboutNeighbors: Updating neighbors for worker {}", targetWorkerId);


    bool success = workerRPCClient->updateNeighbors(targetAddress, neighborInfo);
    if (!success) {
        NES_WARNING("Failed to update neighbors for worker {} at address {}", targetWorkerId, targetAddress);
    } else {
        NES_INFO("Successfully updated neighbors for worker {}", targetWorkerId);
    }
}

}// namespace NES
