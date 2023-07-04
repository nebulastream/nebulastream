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

#ifndef NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_TOPOLOGYCHANGEREQUEST_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_TOPOLOGYCHANGEREQUEST_HPP_

#include <WorkQueues/RequestTypes/Request.hpp>

namespace NES::Experimental {
class RemoveTopologyLinkRequest;
using RemoveTopologyLinkRequestPtr = std::shared_ptr<RemoveTopologyLinkRequest>;

/**
 * @brief This request is used for adding a topology link between two existing topology nodes
 */
class RemoveTopologyLinkRequest : public Request {
  public:
    /**
     * @brief Creates an instance of remove topology link request
     * @param upstreamNodeId: the identifier of upstream topology node
     * @param downstreamNodeId: the identifier of downstream topology node
     * @return a shared pointer to the TopologyChangeRequest
     */
    static RemoveTopologyLinkRequestPtr create(TopologyNodeId upstreamNodeId, TopologyNodeId downstreamNodeId);

    TopologyNodeId getUpstreamNodeId() const;

    TopologyNodeId getDownstreamNodeId() const;

    std::string toString() override;

    RequestType getRequestType() override;

  private:
    explicit RemoveTopologyLinkRequest(TopologyNodeId upstreamNodeId, TopologyNodeId downstreamNodeId);

    NES::TopologyNodeId upstreamNodeId;
    NES::TopologyNodeId downstreamNodeId;
};
}// namespace NES::Experimental
#endif// NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_TOPOLOGYCHANGEREQUEST_HPP_
