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
#include <GRPC/CoordinatorRPCClient.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Services/WorkerHealthCheckService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <thread>

using namespace std::chrono_literals;

namespace NES {

WorkerHealthCheckService::WorkerHealthCheckService(CoordinatorRPCClientPtr coordinatorRpcClient,
                                                   WorkerRPCClientPtr workerRpcClient,
                                                   std::string healthServiceName,
                                                   NesWorkerPtr worker)
    : coordinatorRpcClient(coordinatorRpcClient), workerRpcClient(workerRpcClient), worker(worker) {
    id = coordinatorRpcClient->getId();
    this->healthServiceName = healthServiceName;
}

void WorkerHealthCheckService::startHealthCheck() {
    NES_DEBUG("WorkerHealthCheckService::startHealthCheck worker id= {}", id);
    isRunning = true;
    NES_DEBUG("start health checking on worker");

    healthCheckingOnCoordinatorThread = std::make_shared<std::thread>(([this]() {
        setThreadName("nesHealth");
        NES_DEBUG("NesWorker: start health checking on coordinator");
        auto waitTime = std::chrono::seconds(worker->getWorkerConfiguration()->workerHealthCheckWaitTime.getValue());
        while (isRunning) {
            NES_DEBUG("NesWorker::healthCheck on coordinator for worker id=  {}", coordinatorRpcClient->getId());
            bool isAlive = coordinatorRpcClient->checkCoordinatorHealth(healthServiceName);
            if (isAlive) {
                NES_DEBUG("NesWorker::healthCheck: for worker id={} coordinator is alive", coordinatorRpcClient->getId());
            } else {
                NES_ERROR("NesWorker::healthCheck: for worker id={} coordinator went down so shutting down the worker with ip",
                           coordinatorRpcClient->getId());
                worker->stop(true);
            }
            {
                std::unique_lock<std::mutex> lk(cvMutex);
                cv.wait_for(lk, waitTime, [this] {
                    return isRunning == false;
                });
            }
        }
        //        we have to wait until the code above terminates to proceed afterwards with shutdown of the rpc server (can be delayed due to sleep)
        shutdownRPC->set_value(true);
        NES_DEBUG("NesWorker::healthCheck: stop coordinator health checking id= {}", id);
    }));

    healthCheckingThread = std::make_shared<std::thread>(([this]() {

        setThreadName("nesHealth");
        NES_DEBUG("NesWorker: start health checking on topological neighbors");
        auto waitTime = std::chrono::seconds(worker->getWorkerConfiguration()->workerHealthCheckWaitTime.getValue());
        while (isRunning) {
            NES_DEBUG("NesWorker::topological neighbors healthCheck for worker id=  {}", coordinatorRpcClient->getId());

            // get children data
            auto childrenData = coordinatorRpcClient->getChildrenData(id);
            for (auto data : childrenData) {
                NES_DEBUG("child data: {}", data);
                // Find the position of the first colon ':'
                size_t colonPos = data.find(':');

                // Extract the integer part before the colon
                TopologyNodeId childWorkerId = std::stoi(data.substr(0, colonPos));
                std::string destAddress = data.substr(colonPos + 1);

                children.insert(childWorkerId, destAddress);
            }

            //usleep(1000000);

            for (auto child : children.lock_table()) {
                bool isChildAlive = workerRpcClient->checkHealth(child.second, healthServiceName);
                if (isChildAlive) {
                    NES_DEBUG("NesWorker::healthCheck: child worker with workerId={} is alive", child.first);
                } else {
                    NES_DEBUG("NesWorker::healthCheck: child worker with workerId={} is down", child.first);
                    failedChildrenWorkers.insert(child.first);
                }
            }
            if (!failedChildrenWorkers.empty()) {
                NES_DEBUG("NesWorker::healthCheck: announcing failed children workers to coordinator");
                bool success = coordinatorRpcClient->announceFailedWorkers(id, failedChildrenWorkers);
                if (success) {
                    for (auto failedChildrenWorkerId : failedChildrenWorkers) {
                        children.erase(failedChildrenWorkerId);
                    }
                    failedChildrenWorkers.clear();
                }
            }

            {
                std::unique_lock<std::mutex> lk(cvMutex);
                cv.wait_for(lk, waitTime, [this] {
                    return isRunning == false;
                });
            }
        }
        //        we have to wait until the code above terminates to proceed afterwards with shutdown of the rpc server (can be delayed due to sleep)
        shutdownRPC->set_value(true);
        NES_DEBUG("NesWorker::healthCheck: stop health checking id= {}", id);
    }));
}

}// namespace NES