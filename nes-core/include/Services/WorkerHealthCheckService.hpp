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

#ifndef NES_CORE_INCLUDE_SERVICES_WORKERHEALTHCHECKSERVICE_HPP_
#define NES_CORE_INCLUDE_SERVICES_WORKERHEALTHCHECKSERVICE_HPP_

#include <Catalogs/Topology/AbstractHealthCheckService.hpp>

namespace NES {
class NesWorker;
using NesWorkerPtr = std::shared_ptr<NesWorker>;

/**
 * @brief: This class is responsible for handling requests related to monitor the alive status of nodes from the worker
 */
class WorkerHealthCheckService : public NES::AbstractHealthCheckService {
  public:
    WorkerHealthCheckService(CoordinatorRPCClientPtr coordinatorRpcClient, std::string healthServiceName, NesWorkerPtr worker);

    /**
     * Method to start the health checking
     */
    void startHealthCheck() override;

  private:
    CoordinatorRPCClientPtr coordinatorRpcClient;
    NesWorkerPtr worker;
};

using WorkerHealthCheckServicePtr = std::shared_ptr<WorkerHealthCheckService>;

}// namespace NES

#endif// NES_CORE_INCLUDE_SERVICES_WORKERHEALTHCHECKSERVICE_HPP_
