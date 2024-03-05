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

#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveNodeEvent.hpp>

namespace NES::RequestProcessor {

ISQPRemoveNodeEvent::ISQPRemoveNodeEvent(const WorkerId& workerId,
                                         const std::vector<WorkerId>& downstreamWorkerIds,
                                         const std::vector<WorkerId>& upstreamWorkerIds)
    : ISQPEvent(ISQP_REMOVE_NODE_EVENT_PRIORITY), workerId(workerId), downstreamWorkerIds(downstreamWorkerIds), upstreamWorkerIds(upstreamWorkerIds) {}

ISQPEventPtr ISQPRemoveNodeEvent::create(const WorkerId& workerId,
                                                   const std::vector<WorkerId>& downstreamWorkerIds,
                                                   const std::vector<WorkerId>& upstreamWorkerIds) {
    return std::make_shared<ISQPRemoveNodeEvent>(workerId, downstreamWorkerIds, upstreamWorkerIds);
}

WorkerId ISQPRemoveNodeEvent::getWorkerId() const { return workerId; }

const std::vector<WorkerId>& ISQPRemoveNodeEvent::getDownstreamWorkerIds() const { return downstreamWorkerIds; }

const std::vector<WorkerId>& ISQPRemoveNodeEvent::getUpstreamWorkerIds() const { return upstreamWorkerIds; }

}// namespace NES::RequestProcessor
