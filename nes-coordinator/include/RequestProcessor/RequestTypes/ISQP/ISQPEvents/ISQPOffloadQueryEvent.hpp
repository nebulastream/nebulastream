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

#ifndef ISQPOFFLOADQUERYEVENT_HPP
#define ISQPOFFLOADQUERYEVENT_HPP

#include <Identifiers/Identifiers.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPEvent.hpp>

namespace NES::RequestProcessor {

class ISQPOffloadQueryEvent : public ISQPEvent {
public:
    static ISQPEventPtr create(WorkerId originWorkerId, SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId, WorkerId targetWorkerId);

     ISQPOffloadQueryEvent(WorkerId originWorkerId, SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId, WorkerId targetWorkerId);

    SharedQueryId getSharedQueryId() const;
    WorkerId getTargetWorkerId() const;
    WorkerId getOriginWorkerId() const;
    DecomposedQueryId getDecomposedQueryId() const;

private:
    SharedQueryId sharedQueryId;
    DecomposedQueryId decomposedQueryId;
    WorkerId targetWorkerId;
    WorkerId originWorkerId;
};

using ISQPOffloadQueryEventPtr = std::shared_ptr<ISQPOffloadQueryEvent>;

}

#endif //ISQPOFFLOADQUERYEVENT_HPP
