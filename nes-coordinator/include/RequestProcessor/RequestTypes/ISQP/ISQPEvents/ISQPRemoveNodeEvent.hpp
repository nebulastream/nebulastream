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

#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ISQP_ISQPEVENTS_ISQPREMOVENODEEVENT_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ISQP_ISQPEVENTS_ISQPREMOVENODEEVENT_HPP_

#include <Identifiers/Identifiers.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPEvent.hpp>

namespace NES::RequestProcessor {

/**
 * @brief the response indicating if the remove node event was successfully applied
 */
struct ISQPRemoveNodeResponse : public ISQPResponse {
    explicit ISQPRemoveNodeResponse(bool success) : success(success){};
    bool success;
};
using ISQPRemoveNodeResponsePtr = std::shared_ptr<ISQPRemoveNodeResponse>;

class ISQPRemoveNodeEvent;
using ISQPRemoveNodeEventPtr = std::shared_ptr<ISQPRemoveNodeEvent>;

/**
 * @brief ISQP event indicating the node removal
 */
class ISQPRemoveNodeEvent : public ISQPEvent {

  public:
    static ISQPEventPtr create(const WorkerId& workerId,
                               const std::vector<WorkerId>& downstreamWorkerIds,
                               const std::vector<WorkerId>& upstreamWorkerIds);

    ISQPRemoveNodeEvent(const WorkerId& workerId,
                        const std::vector<WorkerId>& downstreamWorkerIds,
                        const std::vector<WorkerId>& upstreamWorkerIds);

    WorkerId getWorkerId() const;

    const std::vector<WorkerId>& getDownstreamWorkerIds() const;

    const std::vector<WorkerId>& getUpstreamWorkerIds() const;

  private:
    WorkerId workerId;
    std::vector<WorkerId> downstreamWorkerIds;
    std::vector<WorkerId> upstreamWorkerIds;
};
}// namespace NES::RequestProcessor

#endif // NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ISQP_ISQPEVENTS_ISQPREMOVENODEEVENT_HPP_
