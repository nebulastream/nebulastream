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
#pragma once

#include <unordered_set>

#include <Runtime/Execution/OperatorHandler.hpp>
#include <folly/Synchronized.h>
#include <Identifiers/Identifiers.hpp>

namespace NES {

class SNDeduplicationOperatorHandler : public OperatorHandler
{
public:
    SNDeduplicationOperatorHandler(std::string filePath, const std::vector<OriginId>& inputOrigins);

    void start(PipelineExecutionContext&, uint32_t) override
    {

    };

    void stop(QueryTerminationType, PipelineExecutionContext&) override
    {

    }

    void init();

    void save();

    bool checkAndInsert(OriginId originId, SequenceNumber sequenceNumber);



private:
    const std::string filePath;
    const std::vector<OriginId> origins;
    std::unordered_map<OriginId, std::shared_ptr<folly::Synchronized<std::unordered_set<SequenceNumber>>>> seenSequenceNumbers;
};

}