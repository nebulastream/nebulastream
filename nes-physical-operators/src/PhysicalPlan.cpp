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

#include <cstdint>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <PhysicalPlan.hpp>
#include <SourcePhysicalOperator.hpp>

namespace NES
{
PhysicalPlan::PhysicalPlan(
    QueryId id,
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> rootOperators,
    Nautilus::Configurations::ExecutionMode executionMode,
    uint64_t operatorBufferSize)
    : queryId(id), rootOperators(std::move(rootOperators)), executionMode(executionMode), operatorBufferSize(operatorBufferSize)
{
    for (const auto& rootOperator : this->rootOperators)
    {
        PRECONDITION(rootOperator->getPhysicalOperator().tryGet<SourcePhysicalOperator>(), "Expects SourcePhysicalOperator as roots");
    }
}

std::string PhysicalPlan::toString() const
{
    std::stringstream stringstream;
    auto dumpHandler = QueryConsoleDumpHandler<PhysicalPlan, PhysicalOperatorWrapper>(stringstream, true);
    for (const auto& rootOperator : rootOperators)
    {
        dumpHandler.dump(*rootOperator);
    }
    return stringstream.str();
}

QueryId PhysicalPlan::getQueryId() const
{
    return queryId;
}

const PhysicalPlan::Roots& PhysicalPlan::getRootOperators() const
{
    return rootOperators;
}

Nautilus::Configurations::ExecutionMode PhysicalPlan::getExecutionMode() const
{
    return executionMode;
}

uint64_t PhysicalPlan::getOperatorBufferSize() const
{
    return operatorBufferSize;
}

std::ostream& operator<<(std::ostream& os, const PhysicalPlan& plan)
{
    os << plan.toString();
    return os;
}
}
