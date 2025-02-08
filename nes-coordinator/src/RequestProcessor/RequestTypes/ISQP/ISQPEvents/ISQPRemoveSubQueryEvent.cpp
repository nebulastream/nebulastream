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

#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveSubQueryEvent.hpp>

namespace NES::RequestProcessor {

ISQPEventPtr ISQPRemoveSubQueryEvent::create(WorkerId originWorkerId, SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId) {
    return std::make_shared<ISQPRemoveSubQueryEvent>(originWorkerId, sharedQueryId, decomposedQueryId);
}

ISQPRemoveSubQueryEvent::ISQPRemoveSubQueryEvent(WorkerId originWorkerId, SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId)
    : ISQPEvent(ISQP_ADD_NODE_EVENT_PRIORITY), sharedQueryId(sharedQueryId), decomposedQueryId(decomposedQueryId), originWorkerId(originWorkerId) {}

SharedQueryId ISQPRemoveSubQueryEvent::getSharedQueryId() const { return sharedQueryId; }

DecomposedQueryId ISQPRemoveSubQueryEvent::getDecomposedQueryId() const { return decomposedQueryId; }

WorkerId ISQPRemoveSubQueryEvent::getOriginWorkerId() const { return originWorkerId; }
}// namespace NES::RequestProcessor
