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

#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Services/CoordinatorHealthCheckService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>

namespace NES {

CoordinatorHealthCheckService::CoordinatorHealthCheckService(TopologyManagerServicePtr topologyManagerService,
                                                             WorkerRPCClientPtr workerRPCClient,
                                                             std::string healthServiceName,
                                                             Configurations::CoordinatorConfigurationPtr coordinatorConfiguration)
    : topologyManagerService(topologyManagerService), workerRPCClient(workerRPCClient),
      coordinatorConfiguration(coordinatorConfiguration) {
    id = 9999;
    this->healthServiceName = healthServiceName;
}

void CoordinatorHealthCheckService::startHealthCheck() {
    NES_DEBUG2("CoordinatorHealthCheckService::startHealthCheck");
    isRunning = true;
    NES_DEBUG2("start health checking on coordinator");
    healthCheckingThread = std::make_shared<std::thread>(([this]() {
        setThreadName("nesHealth");
        auto waitTime = std::chrono::seconds(this->coordinatorConfiguration->coordinatorHealthCheckWaitTime.getValue());
        while (isRunning) {
            for (auto node : nodeIdToTopologyNodeMap.lock_table()) {
                auto nodeIp = node.second->getIpAddress();
                auto nodeGrpcPort = node.second->getGrpcPort();
                std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);

                //check health
                NES_TRACE2("NesCoordinator::healthCheck: checking node= {}", destAddress);
                auto res = workerRPCClient->checkHealth(destAddress, healthServiceName);
                if (res) {
                    NES_TRACE2("NesCoordinator::healthCheck: node={} is alive", destAddress);
                    removeWorkerFromSetOfInactiveWorkers(node.first);
                } else {
                    NES_WARNING2("NesCoordinator::healthCheck: node={} went dead so we remove it", destAddress);
                    if (topologyManagerService->getRootNode()->getId() == node.second->getId()) {
                        NES_WARNING2("The failing node is the root node so we cannot delete it");
                        shutdownRPC->set_value(true);
                        return;
                    } else {
                        auto ret = topologyManagerService->removePhysicalNode(node.second);
                        addWorkerToSetOfInactiveWorkers(node.first);
                        if (ret) {
                            NES_TRACE2("NesCoordinator::healthCheck: remove node={} successfully", destAddress);
                        } else {
                            NES_WARNING2("Node went offline but could not be removed from topology");
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
        NES_DEBUG2("NesCoordinator: stop health checking");
    }));
}

void CoordinatorHealthCheckService::addWorkerToSetOfInactiveWorkers(uint64_t workerId) {
    NES_DEBUG2("NesCoordinator::healthCheck: adding worker id {} to set of inactive workers.", workerId);
    std::lock_guard<std::mutex> lock(cvMutex2);
    inactiveWorkers.insert(workerId);
    NES_DEBUG2("NesCoordinator::healthCheck: updated set of inactive workers: ");
    for (unsigned long worker : inactiveWorkers) {
        std::cout << worker << " ";
    }
    std::cout << std::endl;
}

void CoordinatorHealthCheckService::removeWorkerFromSetOfInactiveWorkers(uint64_t workerId) {
    NES_DEBUG2("NesCoordinator::healthCheck: removing worker id {} from set of inactive workers.", workerId);
    std::lock_guard<std::mutex> lock(cvMutex2);
    if (inactiveWorkers.contains(workerId)) {
        inactiveWorkers.erase(workerId);
    } else return;
    NES_DEBUG2("NesCoordinator::healthCheck: updated set of inactive workers:\n");
    for (unsigned long worker : inactiveWorkers) {
        std::cout << worker << " ";
    }
    std::cout << std::endl;

}

}// namespace NES