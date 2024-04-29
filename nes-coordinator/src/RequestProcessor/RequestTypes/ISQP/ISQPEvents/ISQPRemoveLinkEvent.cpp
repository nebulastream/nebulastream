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

#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveLinkEvent.hpp>

namespace NES::RequestProcessor {

ISQPRemoveLinkEvent::ISQPRemoveLinkEvent(const WorkerId& parentNodeId, const WorkerId& childWorkerId)
    : ISQPEvent(ISQP_REMOVE_QUERY_EVENT_PRIORITY), parentNodeId(parentNodeId), childNodeId(childWorkerId) {}

ISQPEventPtr ISQPRemoveLinkEvent::create(const WorkerId& parentNodeId, const WorkerId& childWorkerId) {
    return std::make_shared<ISQPRemoveLinkEvent>(parentNodeId, childWorkerId);
}

WorkerId ISQPRemoveLinkEvent::getParentNodeId() const { return parentNodeId; }

WorkerId ISQPRemoveLinkEvent::getChildNodeId() const { return childNodeId; }

}// namespace NES::RequestProcessor
