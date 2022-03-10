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

#ifndef NES_INCLUDE_SERVICES_HEALTHCHECKSERVICE_HPP_
#define NES_INCLUDE_SERVICES_HEALTHCHECKSERVICE_HPP_

#include <memory>
#include <thread>
#include <future>
namespace NES {

class CoordinatorRPCClient;
using CoordinatorRPCClientPtr = std::shared_ptr<CoordinatorRPCClient>;

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class TopologyManagerService;
using TopologyManagerServicePtr = std::shared_ptr<TopologyManagerService>;

/**
 * @brief: This class is responsible for handling requests related to monitor the alive status of nodes
 */
class HealthCheckService {
  public:
    HealthCheckService(TopologyManagerServicePtr topologyManagerService, WorkerRPCClientPtr workerRPCClient);

    HealthCheckService(CoordinatorRPCClientPtr coordinatorRpcClient);

    void startHealthCheck();

    void stopHealthCheck();
  private:
    void checkFromWorkerSide();
    void checkFromCoordinatorSide();

    TopologyManagerServicePtr topologyManagerService;
    WorkerRPCClientPtr workerRPCClient;
    CoordinatorRPCClientPtr coordinatorRpcClient;
    std::shared_ptr<std::thread> healthCheckingThread;
    bool isRunning = false;
    std::shared_ptr<std::promise<bool>> shutdownRPC = std::make_shared<std::promise<bool>>();
};

using HealthCheckServicePtr = std::shared_ptr<HealthCheckService>;

}// namespace NES

#endif// NES_INCLUDE_SERVICES_MONITORINGSERVICE_HPP_
