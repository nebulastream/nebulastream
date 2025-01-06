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

#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPOffloadQueryEvent.hpp>

namespace NES::RequestProcessor {

ISQPEventPtr ISQPOffloadQueryEvent::create(WorkerId originWorkerId, SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId, WorkerId targetWorkerId) {
    return std::make_shared<ISQPOffloadQueryEvent>(originWorkerId, sharedQueryId, decomposedQueryId, targetWorkerId);
}

ISQPOffloadQueryEvent::ISQPOffloadQueryEvent(WorkerId originWorkerId, SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId, WorkerId targetWorkerId)
    : ISQPEvent(ISQP_ADD_NODE_EVENT_PRIORITY), sharedQueryId(sharedQueryId), decomposedQueryId(decomposedQueryId), targetWorkerId(targetWorkerId), originWorkerId(originWorkerId) {}

SharedQueryId ISQPOffloadQueryEvent::getSharedQueryId() const { return sharedQueryId; }

DecomposedQueryId ISQPOffloadQueryEvent::getDecomposedQueryId() const { return decomposedQueryId; }

WorkerId ISQPOffloadQueryEvent::getTargetWorkerId() const { return targetWorkerId; }

WorkerId ISQPOffloadQueryEvent::getOriginWorkerId() const { return originWorkerId; }
}// namespace NES::RequestProcessor
