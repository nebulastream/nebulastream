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

#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ISQP_ISQPEVENTS_ISQPREMOVELINKEVENT_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ISQP_ISQPEVENTS_ISQPREMOVELINKEVENT_HPP_

#include <Identifiers/Identifiers.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPEvent.hpp>
#include <future>

namespace NES::RequestProcessor {

/**
 * @brief the response indicating if the remove link event was successfully applied
 */
struct ISQPRemoveLinkResponse : public ISQPResponse {
    explicit ISQPRemoveLinkResponse(bool success) : success(success){};
    bool success;
};
using ISQPRemoveLinkResponsePtr = std::shared_ptr<ISQPRemoveLinkResponse>;

class ISQPRemoveLinkEvent;
using ISQPRemoveLinkEventPtr = std::shared_ptr<ISQPRemoveLinkEvent>;

/**
 * @brief the ISQP remove link event
 */
class ISQPRemoveLinkEvent : public ISQPEvent {

  public:
    static ISQPEventPtr create(const WorkerId& parentNodeId, const WorkerId& childWorkerId);

    ISQPRemoveLinkEvent(const WorkerId& parentNodeId, const WorkerId& childWorkerId);

    WorkerId getParentNodeId() const;

    WorkerId getChildNodeId() const;

  private:
    WorkerId parentNodeId;
    WorkerId childNodeId;
};
}// namespace NES::RequestProcessor

#endif // NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ISQP_ISQPEVENTS_ISQPREMOVELINKEVENT_HPP_
