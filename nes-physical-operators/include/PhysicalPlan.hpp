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
#include <Nautilus/NautilusBackend.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Pipeline.hpp>

namespace NES
{
/// Stores the physical (executable) plan. This plan holds the output of the query optimizer and the input to the
/// query compiler. It holds the roots of the plan as PhysicalOperatorWrapper containing the actual PhysicalOperator
/// and additional information needed during query compilation.
/// The physical plans operators are required to have the direction source -> sink.
class PhysicalPlan final
{
    using Roots = std::vector<std::shared_ptr<PhysicalOperatorWrapper>>;

public:
    friend std::ostream& operator<<(std::ostream& os, const PhysicalPlan& plan);

    [[nodiscard]] QueryId getQueryId() const;
    [[nodiscard]] const Roots& getRootOperators() const;
    [[nodiscard]] Nautilus::Configurations::ExecutionMode getExecutionMode() const;

private:
    QueryId queryId;
    Roots rootOperators;
    Nautilus::Configurations::ExecutionMode executionMode;

    [[nodiscard]] std::string toString() const;

    friend class PhysicalPlanBuilder;
    PhysicalPlan(QueryId id, Roots rootOperators, Nautilus::Configurations::ExecutionMode executionMode);
};
}
FMT_OSTREAM(NES::PhysicalPlan);
