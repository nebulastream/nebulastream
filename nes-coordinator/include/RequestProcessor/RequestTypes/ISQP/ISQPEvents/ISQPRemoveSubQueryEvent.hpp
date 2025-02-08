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

#ifndef ISQPREMOVESUBQUERYEVENT_HPP
#define ISQPREMOVESUBQUERYEVENT_HPP

#include <Identifiers/Identifiers.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPEvent.hpp>

namespace NES::RequestProcessor {

class ISQPRemoveSubQueryEvent : public ISQPEvent {
public:
    static ISQPEventPtr create(WorkerId originWorkerId, SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId);

    ISQPRemoveSubQueryEvent(WorkerId originWorkerId, SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId);

    SharedQueryId getSharedQueryId() const;
    WorkerId getOriginWorkerId() const;
    DecomposedQueryId getDecomposedQueryId() const;

private:
    SharedQueryId sharedQueryId;
    DecomposedQueryId decomposedQueryId;
    WorkerId originWorkerId;
};

using ISQPRemoveSubQueryEventPtr = std::shared_ptr<ISQPRemoveSubQueryEvent>;

}

#endif //ISQPOFFLOADQUERYEVENT_HPP
