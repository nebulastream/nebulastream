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

#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddLinkPropertyEvent.hpp>

namespace NES::RequestProcessor {

ISQPEventPtr ISQPAddLinkPropertyEvent::create(const WorkerId& parentNodeId,
                                              const WorkerId& childWorkerId,
                                              const uint64_t& bandwidth,
                                              const uint64_t& latency) {
    return std::make_shared<ISQPAddLinkPropertyEvent>(parentNodeId, childWorkerId, bandwidth, latency);
};

ISQPAddLinkPropertyEvent::ISQPAddLinkPropertyEvent(const WorkerId& parentNodeId,
                                                   const WorkerId& childWorkerId,
                                                   const uint64_t& bandwidth,
                                                   const uint64_t& latency)
    : ISQPEvent(ISQP_ADD_LINK_EVENT_PRIORITY), parentNodeId(parentNodeId), childNodeId(childWorkerId), bandwidth(bandwidth),
      latency(latency) {}

WorkerId ISQPAddLinkPropertyEvent::getParentNodeId() const { return parentNodeId; }

WorkerId ISQPAddLinkPropertyEvent::getChildNodeId() const { return childNodeId; }

uint64_t ISQPAddLinkPropertyEvent::getBandwidth() const { return bandwidth; }

uint64_t ISQPAddLinkPropertyEvent::getLatency() const { return latency; }

}// namespace NES::RequestProcessor
