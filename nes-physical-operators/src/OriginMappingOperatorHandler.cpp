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

#include <OriginMappingOperatorHandler.hpp>

#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

OriginMappingOperatorHandler::OriginMappingOperatorHandler(const std::vector<std::pair<OriginId, OriginId>>& originMapping)
{
    this->originMapping.reserve(originMapping.size());
    for (const auto& [upstreamOriginId, branchOriginId] : originMapping)
    {
        this->originMapping.emplace(upstreamOriginId, branchOriginId);
    }
}

void OriginMappingOperatorHandler::start(PipelineExecutionContext&)
{
}

void OriginMappingOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
}

OriginId OriginMappingOperatorHandler::mapOrigin(const OriginId originId) const
{
    const auto branchOriginId = originMapping.find(originId);
    INVARIANT(branchOriginId != originMapping.end(), "Buffer with origin {} reached a branch that was not told about it", originId);
    return branchOriginId->second;
}

}
