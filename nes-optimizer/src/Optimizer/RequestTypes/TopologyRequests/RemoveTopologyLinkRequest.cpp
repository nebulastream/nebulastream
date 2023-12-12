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

#include <Optimizer/RequestTypes/TopologyRequests/RemoveTopologyLinkRequest.hpp>
#include <string>

namespace NES::Experimental {

RemoveTopologyLinkRequestPtr RemoveTopologyLinkRequest::create(WorkerId downstreamNodeId, WorkerId upstreamNodeId) {
    return std::make_shared<RemoveTopologyLinkRequest>(RemoveTopologyLinkRequest(downstreamNodeId, upstreamNodeId));
}

RemoveTopologyLinkRequest::RemoveTopologyLinkRequest(WorkerId downstreamNodeId, WorkerId upstreamNodeId)
    : downstreamNodeId(downstreamNodeId), upstreamNodeId(upstreamNodeId) {}

WorkerId RemoveTopologyLinkRequest::getUpstreamNodeId() const { return upstreamNodeId; }

WorkerId RemoveTopologyLinkRequest::getDownstreamNodeId() const { return downstreamNodeId; }

std::string RemoveTopologyLinkRequest::toString() {
    return "RemoveTopologyLinkRequest { UpstreamNodeId: " + std::to_string(upstreamNodeId)
        + ", DownstreamNodeId: " + std::to_string(downstreamNodeId) + "}";
}

RequestType RemoveTopologyLinkRequest::getRequestType() { return RequestType::RemoveTopologyLink; }
}// namespace NES::Experimental
