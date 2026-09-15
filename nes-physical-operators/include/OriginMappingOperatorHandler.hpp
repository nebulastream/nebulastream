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

#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>

namespace NES
{

class PipelineExecutionContext;

/// What one branch of a fan-out point stamps its buffers with: every origin id the branch reads, mapped to the id that
/// replaces it. The scan heading the branch asks this handler for each buffer it opens, so that the branch's records
/// travel on under an identity no other branch carries.
class OriginMappingOperatorHandler final : public OperatorHandler
{
public:
    explicit OriginMappingOperatorHandler(const std::vector<std::pair<OriginId, OriginId>>& originMapping);

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    /// The id that replaces the one a buffer carries. An id the branch was not told about is a bug in the optimizer:
    /// the mapping is built from the origin ids the child forwards.
    [[nodiscard]] OriginId mapOrigin(OriginId originId) const;

private:
    std::unordered_map<OriginId, OriginId> originMapping;
};

}
