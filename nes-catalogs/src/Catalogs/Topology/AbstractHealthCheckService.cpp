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

#include <Catalogs/Topology/AbstractHealthCheckService.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {
AbstractHealthCheckService::AbstractHealthCheckService() {}

void AbstractHealthCheckService::stopHealthCheck() {
    NES_DEBUG("AbstractHealthCheckService::stopHealthCheck called on id= {}", id);
    auto expected = true;
    if (!isRunning.compare_exchange_strong(expected, false)) {
        NES_DEBUG("AbstractHealthCheckService::stopHealthCheck health check already stopped");
        return;
    }
    {
        std::unique_lock<std::mutex> lk(cvMutex);
        cv.notify_all();
    }
    auto ret = shutdownRPC->get_future().get();
    NES_ASSERT(ret, "fail to shutdown health check");

    if (healthCheckingThread->joinable()) {
        healthCheckingThread->join();
        healthCheckingThread.reset();
        NES_DEBUG("AbstractHealthCheckService::stopHealthCheck successfully stopped");
    } else {
        NES_ERROR("HealthCheckService: health thread not joinable");
        NES_THROW_RUNTIME_ERROR("Error while stopping healthCheckingThread->join");
    }
}

void AbstractHealthCheckService::addNodeToHealthCheck(WorkerId workerId, const std::string& rpcAddress) {
    NES_DEBUG("HealthCheckService: adding node with id {}", workerId);
    auto exists = topologyIdToRPCAddressMap.contains(workerId);
    if (exists) {
        NES_THROW_RUNTIME_ERROR("HealthCheckService want to add node that already exists id=" << workerId);
    }
    topologyIdToRPCAddressMap.insert(workerId, rpcAddress);
}

void AbstractHealthCheckService::removeNodeFromHealthCheck(WorkerId workerId) {
    if (!topologyIdToRPCAddressMap.contains(workerId)) {
        NES_THROW_RUNTIME_ERROR("HealthCheckService want to remove a node that does not exists id=" << workerId);
    }
    NES_DEBUG("HealthCheckService: removing node with id {}", workerId);
    topologyIdToRPCAddressMap.erase(workerId);
}

bool AbstractHealthCheckService::getRunning() { return isRunning; }

bool AbstractHealthCheckService::isWorkerInactive(WorkerId workerId) {
    NES_DEBUG("HealthCheckService: checking if node with id {} is inactive", workerId);
    std::lock_guard<std::mutex> lock(cvMutex);
    bool isNotActive = inactiveWorkers.contains(workerId);
    if (isNotActive) {
        NES_DEBUG("HealthCheckService: node with id {} is inactive", workerId);
        return true;
    }
    NES_DEBUG("HealthCheckService: node with id {} is active", workerId);
    return false;
}

}// namespace NES