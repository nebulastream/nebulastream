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

#include <Services/AbstractHealthCheckService.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Topology/TopologyNode.hpp>

namespace NES {
AbstractHealthCheckService::AbstractHealthCheckService()
{

}

void AbstractHealthCheckService::stopHealthCheck() {
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
//
//void AbstractHealthCheckService::checkFromWorkerSide() {
//    isRunning = true;
//    NES_DEBUG("start health checking on worker");
//    healthCheckingThread = std::make_shared<std::thread>(([this]() {
//        setThreadName("nesHealth");
//
//        NES_DEBUG("NesWorker: start health checking");
//        while (isRunning) {
//            NES_DEBUG("NesWorker::healthCheck for worker id= " << coordinatorRpcClient->getId());
//
//            bool isAlive = coordinatorRpcClient->checkCoordinatorHealth();
//            if (isAlive) {
//                NES_DEBUG("NesWorker::healthCheck: for worker id=" << coordinatorRpcClient->getId() << " is alive");
//            } else {
//                NES_ERROR("NesWorker::healthCheck: for worker id="
//                          << coordinatorRpcClient->getId() << " coordinator went down so shutting down the worker with ip");
//                std::exit(-1);
//            }
//            std::this_thread::sleep_for(std::chrono::seconds(waitTimeInSeconds));
//        }
//        //        we have to wait until the code above terminates to proceed afterwards with shutdown of the rpc server (can be delayed due to sleep)
//        shutdownRPC->set_value(true);
//        NES_DEBUG("NesWorker::healthCheck: stop health checking");
//    }));
//}
//
//void AbstractHealthCheckService::checkFromCoordinatorSide() {
//    isRunning = true;
//    NES_DEBUG("start health checking on coordinator");
//    healthCheckingThread = std::make_shared<std::thread>(([this]() {
//        setThreadName("nesHealth");
//
//        while (isRunning) {
//            for (auto node : nodeIdToTopologyNodeMap) {
//                auto nodeIp = node.second->getIpAddress();
//                auto nodeGrpcPort = node.second->getGrpcPort();
//                std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);
//
//                //check health
//                NES_DEBUG("NesCoordinator::healthCheck: checking node=" << destAddress);
//                auto res = workerRPCClient->checkHealth(destAddress);
//                if (res) {
//                    NES_DEBUG("NesCoordinator::healthCheck: node=" << destAddress << " is alive");
//                } else {
//                    NES_ERROR("NesCoordinator::healthCheck: node=" << destAddress << " went dead so we remove it");
//                    if (topologyManagerService->getRootNode()->getId() == node.second->getId()) {
//                        NES_WARNING("The failing node is the root node so we cannot delete it");
//                        shutdownRPC->set_value(true);
//                        return;
//                    } else {
//                        auto ret = topologyManagerService->removePhysicalNode(node.second);
//                        if (ret) {
//                            NES_DEBUG("NesCoordinator::healthCheck: remove node =" << destAddress << " successfully");
//                        } else {
//                            NES_THROW_RUNTIME_ERROR("Node went offline but could not be removed from topology");
//                        }
//                        //Source catalog
//                        //                    SourceCatalogService unregisterPhysicalSource
//
//                        //TODO Create issue for remove and child
//                    }
//                }
//            }
//
//            std::this_thread::sleep_for(std::chrono::seconds(waitTimeInSeconds));
//        }
//        shutdownRPC->set_value(true);
//        NES_DEBUG("NesCo547ordinator: stop health checking");
//    }));
//}


void AbstractHealthCheckService::addNodeToHealthCheck(TopologyNodePtr node)
{
    if(nodeIdToTopologyNodeMap.find(node->getId()) == nodeIdToTopologyNodeMap.end())
    {
        NES_THROW_RUNTIME_ERROR("HealthCheckService want to add node that already exists id=" << node->getId());
    }

    NES_DEBUG("HealthCheckService: adding node with id " << node->getId());
    nodeIdToTopologyNodeMap[node->getId()] = node;
}

void AbstractHealthCheckService::removeNodeFromHealthCheck(TopologyNodePtr node)
{
    if(nodeIdToTopologyNodeMap.find(node->getId()) == nodeIdToTopologyNodeMap.end())
    {
        NES_THROW_RUNTIME_ERROR("HealthCheckService want to remove a node that does not exists id=" << node->getId());
    }
    NES_DEBUG("HealthCheckService: removing node with id " << node->getId());
    nodeIdToTopologyNodeMap.erase(nodeIdToTopologyNodeMap.find(node->getId()));
}

}// namespace NES