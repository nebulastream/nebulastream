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

#ifndef NES_WORKER_INCLUDE_SERVICES_WORKERHEALTHCHECKSERVICE_HPP_
#define NES_WORKER_INCLUDE_SERVICES_WORKERHEALTHCHECKSERVICE_HPP_

#include <string>

namespace NES {
class NesWorker;
using NesWorkerPtr = std::shared_ptr<NesWorker>;

/**
 * @brief: This class is responsible for handling requests related to monitor the alive status of nodes from the worker
 */
class WorkerHealthCheckService {
  public:
    WorkerHealthCheckService(CoordinatorRPCClientPtr coordinatorRpcClient, std::string healthServiceName, NesWorkerPtr worker);

    /**
     * Method to start the health checking
     */
    void startHealthCheck();

    void stopHealthCheck();

  private:
    CoordinatorRPCClientPtr coordinatorRpcClient;
    NesWorkerPtr worker;
    std::shared_ptr<std::thread> healthCheckingThread;
    std::atomic<bool> isRunning = false;
    uint64_t id;
    std::string healthServiceName;
    std::mutex cvMutex;
    std::shared_ptr<std::promise<bool>> shutdownRPC = std::make_shared<std::promise<bool>>();
    std::condition_variable cv;
};

using WorkerHealthCheckServicePtr = std::shared_ptr<WorkerHealthCheckService>;

}// namespace NES

#endif// NES_WORKER_INCLUDE_SERVICES_WORKERHEALTHCHECKSERVICE_HPP_
