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
namespace NES {

WorkerHealthCheckService::WorkerHealthCheckService(CoordinatorRPCClientPtr coordinatorRpcClient,
                                                   std::string healthServiceName,
                                                   NesWorkerPtr worker)
    : coordinatorRpcClient(coordinatorRpcClient), worker(worker){
    id = coordinatorRpcClient->getId();
    this->healthServiceName = healthServiceName;
}

void WorkerHealthCheckService::startHealthCheck() {
    NES_DEBUG("WorkerHealthCheckService::startHealthCheck worker id=" << id);
    isRunning = true;
    NES_DEBUG("start health checking on worker");
    healthCheckingThread = std::make_shared<std::thread>(([this]() {
        setThreadName("nesHealth");

        NES_DEBUG("NesWorker: start health checking");
        while (isRunning) {
            NES_TRACE("NesWorker::healthCheck for worker id= " << coordinatorRpcClient->getId());

            bool isAlive = coordinatorRpcClient->checkCoordinatorHealth(healthServiceName);
            if (isAlive) {
                NES_DEBUG("NesWorker::healthCheck: for worker id=" << coordinatorRpcClient->getId() << " is alive");
            } else {
                NES_WARNING("NesWorker::healthCheck: for worker id="
                            << coordinatorRpcClient->getId() << " coordinator went down so shutting down the worker with ip");
                worker->stop(true);
            }
            std::this_thread::sleep_for(std::chrono::seconds(waitTimeInSeconds));
        }
        //        we have to wait until the code above terminates to proceed afterwards with shutdown of the rpc server (can be delayed due to sleep)
        shutdownRPC->set_value(true);
        NES_DEBUG("NesWorker::healthCheck: stop health checking id=" << id);
    }));
}

}// namespace NES