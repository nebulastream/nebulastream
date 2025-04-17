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

#include <vector>
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Pipeline.hpp>

namespace NES
{
/// Representation of a query plan of pipelines
struct PipelinedQueryPlan
{
    PipelinedQueryPlan(QueryId id);
    [[nodiscard]] std::string toString() const;
    friend std::ostream& operator<<(std::ostream& os, const PipelinedQueryPlan& t);
    [[nodiscard]] std::vector<std::shared_ptr<Pipeline>> getSourcePipelines() const;
    [[nodiscard]] std::vector<std::shared_ptr<Pipeline>> getSinkPipelines() const;

    const QueryId queryId;
    std::vector<std::shared_ptr<Pipeline>> pipelines;
};
}
FMT_OSTREAM(NES::PipelinedQueryPlan);
