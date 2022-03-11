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

#include <Services/CoordinatorHealthCheckService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <Services/TopologyManagerService.hpp>
#include <GRPC/WorkerRPCClient.hpp>

namespace NES {

CoordinatorHealthCheckService::CoordinatorHealthCheckService(TopologyManagerServicePtr topologyManagerService,
                                                             WorkerRPCClientPtr workerRPCClient)
    : topologyManagerService(topologyManagerService), workerRPCClient(workerRPCClient) {}

void CoordinatorHealthCheckService::startHealthCheck() {
    isRunning = true;
    NES_DEBUG("start health checking on coordinator");
    healthCheckingThread = std::make_shared<std::thread>(([this]() {
        setThreadName("nesHealth");

        while (isRunning) {
            for (auto node : nodeIdToTopologyNodeMap) {
                auto nodeIp = node.second->getIpAddress();
                auto nodeGrpcPort = node.second->getGrpcPort();
                std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);

                //check health
                NES_DEBUG("NesCoordinator::healthCheck: checking node=" << destAddress);
                auto res = workerRPCClient->checkHealth(destAddress);
                if (res) {
                    NES_DEBUG("NesCoordinator::healthCheck: node=" << destAddress << " is alive");
                } else {
                    NES_ERROR("NesCoordinator::healthCheck: node=" << destAddress << " went dead so we remove it");
                    if (topologyManagerService->getRootNode()->getId() == node.second->getId()) {
                        NES_WARNING("The failing node is the root node so we cannot delete it");
                        shutdownRPC->set_value(true);
                        return;
                    } else {
                        auto ret = topologyManagerService->removePhysicalNode(node.second);
                        if (ret) {
                            NES_DEBUG("NesCoordinator::healthCheck: remove node =" << destAddress << " successfully");
                        } else {
                            NES_DEBUG("Node went offline but could not be removed from topology");
                        }
                        //Source catalog
                        //                    SourceCatalogService unregisterPhysicalSource

                        //TODO Create issue for remove and child
                    }
                }
            }

            std::this_thread::sleep_for(std::chrono::seconds(waitTimeInSeconds));
        }
        shutdownRPC->set_value(true);
        NES_DEBUG("NesCo547ordinator: stop health checking");
    }));
}

}// namespace NES