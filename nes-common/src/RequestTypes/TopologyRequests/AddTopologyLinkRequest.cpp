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

#include <WorkQueues/RequestTypes/TopologyRequests/AddTopologyLinkRequest.hpp>
#include <string>

namespace NES::Experimental {

AddTopologyLinkRequestPtr AddTopologyLinkRequest::create(NES::TopologyNodeId upstreamNodeId,
                                                         NES::TopologyNodeId downstreamNodeId) {
    return std::make_shared<AddTopologyLinkRequest>(AddTopologyLinkRequest(upstreamNodeId, downstreamNodeId));
}

AddTopologyLinkRequest::AddTopologyLinkRequest(NES::TopologyNodeId upstreamNodeId, NES::TopologyNodeId downstreamNodeId)
    : upstreamNodeId(upstreamNodeId), downstreamNodeId(downstreamNodeId) {}

TopologyNodeId AddTopologyLinkRequest::getUpstreamNodeId() const { return upstreamNodeId; }

TopologyNodeId AddTopologyLinkRequest::getDownstreamNodeId() const { return downstreamNodeId; }

std::string AddTopologyLinkRequest::toString() {
    return "AddTopologyLinkRequest { UpstreamNodeId: " + std::to_string(upstreamNodeId)
        + ", DownstreamNodeId: " + std::to_string(downstreamNodeId) + "}";
}

RequestType AddTopologyLinkRequest::getRequestType() { return RequestType::AddTopologyLink; }

}// namespace NES::Experimental
