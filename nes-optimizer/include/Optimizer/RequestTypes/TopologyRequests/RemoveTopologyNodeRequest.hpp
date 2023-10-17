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

#ifndef NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_TOPOLOGYREQUESTS_REMOVETOPOLOGYNODEREQUEST_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_TOPOLOGYREQUESTS_REMOVETOPOLOGYNODEREQUEST_HPP_

#include  <Optimizer/RequestTypes/Request.hpp>

namespace NES::Experimental {

class RemoveTopologyNodeRequest;
using RemoveTopologyNodeRequestPtr = std::shared_ptr<RemoveTopologyNodeRequest>;

/**
 * @brief This request is used for removing a topology node from the topology
 */
class RemoveTopologyNodeRequest : public Request {
  public:
    /**
     * @brief Topology change request that represent changes that are occurring into the underlying infrastructure
     * @param topologyNodeId: topology node to remove
     * @return a shared pointer to the RemoveTopologyNodeRequest
     */
    static RemoveTopologyNodeRequestPtr create(TopologyNodeId topologyNodeId);

    /**
     * @brief Get the id of the topology that was removed
     * @return : topology id
     */
    TopologyNodeId getTopologyNodeId() const;

    std::string toString() override;

    RequestType getRequestType() override;

  private:
    explicit RemoveTopologyNodeRequest(TopologyNodeId topologyNodeId);

    TopologyNodeId topologyNodeId;
};
}// namespace NES::Experimental
#endif// NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_TOPOLOGYREQUESTS_REMOVETOPOLOGYNODEREQUEST_HPP_
