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
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Pipeline.hpp>

namespace NES
{
/// Stores the physical (executable) plan. This plan holds the output of the query optimizer and the input to the
/// query compiler. It holds the roots of the plan as PhysicalOperatorWrapper containing the actual PhysicalOperator
/// and additional information needed during query compilation.
struct PhysicalPlan final
{
    PhysicalPlan(QueryId id, std::vector<std::shared_ptr<PhysicalOperatorWrapper>> rootOperators);
    [[nodiscard]] std::string toString() const;
    friend std::ostream& operator<<(std::ostream& os, const PhysicalPlan& t);

    /// Indicates the node direction
    enum class PlanDirection
    {
        SourceToSink, /// Execution: from data sources to sinks
        SinkToSource /// Optimization: from sinks to source
    };

    const QueryId queryId;
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> rootOperators;
    const PlanDirection direction;

private:
    /// Private constructor that allows a different direction; used by flip()
    PhysicalPlan(QueryId id, std::vector<std::shared_ptr<PhysicalOperatorWrapper>> rootOperators, PlanDirection dir);

    friend PhysicalPlan flip(PhysicalPlan plan);
};

/// free function to flip a PhysicalPlan from SinkToSource to SourceToSink direction
PhysicalPlan flip(PhysicalPlan plan);

}
FMT_OSTREAM(NES::PhysicalPlan);
