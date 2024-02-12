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
                                                   std::string healthServiceName,
                                                   NesWorkerPtr worker)
    : coordinatorRpcClient(coordinatorRpcClient), worker(worker), id(coordinatorRpcClient->getId()),
      healthServiceName(healthServiceName) {}

void WorkerHealthCheckService::startHealthCheck() {
    NES_DEBUG("WorkerHealthCheckService::startHealthCheck worker id= {}", id);
    isRunning = true;
    NES_DEBUG("start health checking on worker");
    healthCheckingThread = std::make_shared<std::thread>(([this]() {
        setThreadName("nesHealth");
        NES_TRACE("NesWorker: start health checking");
        auto waitTime = std::chrono::seconds(worker->getWorkerConfiguration()->workerHealthCheckWaitTime.getValue());
        while (isRunning) {
            NES_TRACE("NesWorker::healthCheck for worker id=  {}", coordinatorRpcClient->getId());

            if (coordinatorRpcClient->checkCoordinatorHealth(healthServiceName)) {
                NES_TRACE("NesWorker::healthCheck: for worker id={} is alive", coordinatorRpcClient->getId());
            } else {
                NES_ERROR("NesWorker::healthCheck: for worker id={} coordinator went down so shutting down the worker with ip",
                          coordinatorRpcClient->getId());
                worker->stop(true);
            }
            {
                std::unique_lock<std::mutex> lk(cvMutex);
                cv.wait_for(lk, waitTime, [this] {
                    return !isRunning;
                });
            }
        }
        //        we have to wait until the code above terminates to proceed afterwards with shutdown of the rpc server (can be delayed due to sleep)
        shutdownRPC->set_value(true);
        NES_DEBUG("NesWorker::healthCheck: stop health checking id= {}", id);
    }));
}

void WorkerHealthCheckService::stopHealthCheck() {
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

}// namespace NES