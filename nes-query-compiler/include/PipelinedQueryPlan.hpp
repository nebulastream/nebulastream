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

#include <memory>
#include <ostream>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Pipeline.hpp>


namespace NES
{
struct PipelinedQueryPlan final
{
    explicit PipelinedQueryPlan(QueryId id, Nautilus::Configurations::ExecutionMode executionMode);

    friend std::ostream& operator<<(std::ostream& os, const PipelinedQueryPlan& plan);

    [[nodiscard]] QueryId getQueryId() const;
    [[nodiscard]] Nautilus::Configurations::ExecutionMode getExecutionMode() const;

    [[nodiscard]] std::vector<std::shared_ptr<Pipeline>> getSourcePipelines() const;
    [[nodiscard]] const std::vector<std::shared_ptr<Pipeline>>& getPipelines() const;
    void addPipeline(const std::shared_ptr<Pipeline>& pipeline);
    void removePipeline(Pipeline& pipeline);

private:
    QueryId queryId;
    Nautilus::Configurations::ExecutionMode executionMode;
    std::vector<std::shared_ptr<Pipeline>> pipelines;
};
}
FMT_OSTREAM(NES::PipelinedQueryPlan);
