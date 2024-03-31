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

#ifndef NES_ISQPADDLINKPROPERTYEVENT_HPP
#define NES_ISQPADDLINKPROPERTYEVENT_HPP

#include <Identifiers.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPEvent.hpp>

namespace NES::RequestProcessor {

/**
 * @brief the response indicating if the add link event was successfully applied
 */
struct ISQPAddLinkPropertyResponse : public ISQPResponse {
    explicit ISQPAddLinkPropertyResponse(bool success) : success(success){};
    bool success;
};
using ISQPAddLinkPropertyResponsePtr = std::shared_ptr<ISQPAddLinkPropertyResponse>;

class ISQPAddLinkPropertyEvent;
using ISQPAddLinkPropertyEventPtr = std::shared_ptr<ISQPAddLinkPropertyEvent>;

/**
 * @brief Representing the ISQP add link event indicating a new link is established between two worker node ids
 */
class ISQPAddLinkPropertyEvent : public ISQPEvent {

  public:
    static ISQPEventPtr
    create(const WorkerId& parentNodeId, const WorkerId& childWorkerId, const uint64_t& bandwidth, const uint64_t& latency);

    ISQPAddLinkPropertyEvent(const WorkerId& parentNodeId,
                             const WorkerId& childWorkerId,
                             const uint64_t& bandwidth,
                             const uint64_t& latency);

    WorkerId getParentNodeId() const;

    WorkerId getChildNodeId() const;

    uint64_t getBandwidth() const;

    uint64_t getLatency() const;

  private:
    WorkerId parentNodeId;
    WorkerId childNodeId;
    uint64_t bandwidth;
    uint64_t latency;
};
}// namespace NES::RequestProcessor

#endif//NES_ISQPADDLINKEVENT_HPP
