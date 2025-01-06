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

#ifndef NES_COORDINATOR_INCLUDE_SERVICES_COORDINATORHEALTHCHECKSERVICE_HPP_
#define NES_COORDINATOR_INCLUDE_SERVICES_COORDINATORHEALTHCHECKSERVICE_HPP_

#include <Catalogs/Topology/AbstractHealthCheckService.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Util/libcuckoo/cuckoohash_map.hh>
#include <stdint.h>

namespace NES {

/**
 * @brief: This class is responsible for handling requests related to monitor the alive status of nodes from the coordinator
 */
class CoordinatorHealthCheckService : public NES::AbstractHealthCheckService {
  public:
    CoordinatorHealthCheckService(TopologyPtr topology,
                                  std::string healthServiceName,
                                  Configurations::CoordinatorConfigurationPtr coordinatorConfiguration);

    /**
     * Method to start the health checking
     */
    void startHealthCheck() override;

  void informWorkerAboutNeighbors(WorkerId targetWorkerId, std::string targetAddress, std::vector<std::pair<WorkerId, std::string>> neighborInfo);
  private:
    TopologyPtr topology;
    WorkerRPCClientPtr workerRPCClient;
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
};

using CoordinatorHealthCheckServicePtr = std::shared_ptr<CoordinatorHealthCheckService>;

}// namespace NES

#endif// NES_COORDINATOR_INCLUDE_SERVICES_COORDINATORHEALTHCHECKSERVICE_HPP_
