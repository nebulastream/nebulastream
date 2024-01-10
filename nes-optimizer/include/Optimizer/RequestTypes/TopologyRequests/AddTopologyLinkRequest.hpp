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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_REQUESTTYPES_TOPOLOGYREQUESTS_ADDTOPOLOGYLINKREQUEST_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_REQUESTTYPES_TOPOLOGYREQUESTS_ADDTOPOLOGYLINKREQUEST_HPP_

#include  <Optimizer/RequestTypes/Request.hpp>
#include <future>

namespace NES::Experimental {

class AddTopologyLinkRequest;
using AddTopologyLinkRequestPtr = std::shared_ptr<AddTopologyLinkRequest>;

/**
 * @brief This request is used for adding a topology link between two existing topology nodes
 */
class AddTopologyLinkRequest : public Request {
  public:
    /**
     * @brief Creates an instance of add topology link request
     * @param upstreamNodeId: the identifier of upstream topology node
     * @param downstreamNodeId: the identifier of downstream topology node
     * @return a shared pointer to the TopologyChangeRequest
     */
    static AddTopologyLinkRequestPtr create(WorkerId upstreamNodeId, WorkerId downstreamNodeId);

    /**
     * @brief get identifier of the upstream topology node
     * @return topology node id
     */
    WorkerId getUpstreamNodeId() const;

    /**
     * @brief get identifier of the downstream topology node
     * @return topology node id
     */
    WorkerId getDownstreamNodeId() const;

    std::string toString() override;

    RequestType getRequestType() override;

  private:
    explicit AddTopologyLinkRequest(WorkerId upstreamNodeId, WorkerId downstreamNodeId);

    NES::WorkerId upstreamNodeId;
    NES::WorkerId downstreamNodeId;
};
}// namespace NES::Experimental
#endif // NES_OPTIMIZER_INCLUDE_OPTIMIZER_REQUESTTYPES_TOPOLOGYREQUESTS_ADDTOPOLOGYLINKREQUEST_HPP_
