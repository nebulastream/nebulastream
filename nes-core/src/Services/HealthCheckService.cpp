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

#include <GRPC/CoordinatorRPCClient.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Services/HealthCheckService.hpp>
#include <Services/TopologyManagerService.hpp>
//#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
const uint64_t waitTimeInSeconds = 1;

namespace NES {
HealthCheckService::HealthCheckService(TopologyManagerServicePtr topologyManagerService, WorkerRPCClientPtr workerRPCClient)
    : topologyManagerService(topologyManagerService), workerRPCClient(workerRPCClient) {}

HealthCheckService::HealthCheckService(CoordinatorRPCClientPtr coordinatorRpcClient)
    : coordinatorRpcClient(coordinatorRpcClient) {}

void HealthCheckService::startHealthCheck() {
    if (workerRPCClient) {
        checkFromCoordinatorSide();
    } else {
        checkFromWorkerSide();
    }
}

void HealthCheckService::stopHealthCheck() {
    isRunning = false;
    auto ret = shutdownRPC->get_future().get();
    NES_ASSERT(ret, "fail to shutdown health check");

    if (healthCheckingThread->joinable()) {
        healthCheckingThread->join();
        healthCheckingThread.reset();
    } else {
        NES_ERROR("HealthCheckService: health thread not joinable");
        NES_THROW_RUNTIME_ERROR("Error while stopping healthCheckingThread->join");
    }
}

void HealthCheckService::checkFromWorkerSide() {
    isRunning = true;
    NES_DEBUG("start health checking on worker");
    healthCheckingThread = std::make_shared<std::thread>(([this]() {
        setThreadName("nesHealth");

        NES_DEBUG("NesWorker: start health checking");
        while (isRunning) {
            NES_DEBUG("NesWorker::healthCheck for worker id= " << coordinatorRpcClient->getId());

            bool isAlive = coordinatorRpcClient->checkCoordinatorHealth();
            if (isAlive) {
                NES_DEBUG("NesWorker::healthCheck: for worker id=" << coordinatorRpcClient->getId() << " is alive");
            } else {
                NES_ERROR("NesWorker::healthCheck: for worker id=" << coordinatorRpcClient->getId()
                                                                   << " coordinator went down so shutting down the worker");
                std::exit(-1);
            }
            std::this_thread::sleep_for(std::chrono::seconds(waitTimeInSeconds));
        }
        shutdownRPC->set_value(true);
        NES_DEBUG("NesWorker::healthCheck: stop health checking");
    }));
}

void HealthCheckService::checkFromCoordinatorSide() {
    isRunning = true;
    NES_DEBUG("start health checking on coordinator");
    healthCheckingThread = std::make_shared<std::thread>(([this]() {
        setThreadName("nesHealth");

        while (isRunning) {
            NES_DEBUG("NesCoordinator: start health checking");
            auto root = topologyManagerService->getRootNode();
            auto topologyIterator = NES::DepthFirstNodeIterator(root).begin();
            while (topologyIterator != NES::DepthFirstNodeIterator::end()) {
                //get node
                auto currentTopologyNode = (*topologyIterator)->as<TopologyNode>();

                //get Address
                auto nodeIp = currentTopologyNode->getIpAddress();
                auto nodeGrpcPort = currentTopologyNode->getGrpcPort();
                std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);

                //check health
                NES_DEBUG("NesCoordinator::healthCheck: checking node=" << destAddress);
                auto res = workerRPCClient->checkHealth(destAddress);
                if (res) {
                    NES_DEBUG("NesCoordinator::healthCheck: node=" << destAddress << " is alive");
                } else {
                    NES_ERROR("NesCoordinator::healthCheck: node=" << destAddress << " went dead so we remove it");
                    auto ret = topologyManagerService->removePhysicalNode(currentTopologyNode);
                    if (ret) {
                        NES_DEBUG("NesCoordinator::healthCheck: remove node =" << destAddress << " successfully");
                    } else {
                        NES_THROW_RUNTIME_ERROR("Node went offline but could not be removed from topology");
                    }
                    //Source catalog
//                    SourceCatalogService unregisterPhysicalSource

                    //TODO Create issue for remove and child
                }
                ++topologyIterator;
            }

            std::this_thread::sleep_for(std::chrono::seconds(waitTimeInSeconds));
        }
        shutdownRPC->set_value(true);
        NES_DEBUG("NesCo547ordinator: stop health checking");
    }));
}

}// namespace NES