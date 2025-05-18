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
/// Represents a query plan composed of interconnected pipelines.
///
/// This class serves as a container for a collection of @link Pipeline objects that
/// together define the data flow and processing stages of a query. It holds metadata
/// such as the query ID and execution mode. It provides functionalities to manage
/// these pipelines, such as adding, removing, and retrieving them, particularly
/// identifying source pipelines which act as entry points for data.
/// This representation is used during the query compilation process (@link QueryCompiler) to transform
/// a @link PhysicalPlan into @link CompiledQueryPlan.
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
