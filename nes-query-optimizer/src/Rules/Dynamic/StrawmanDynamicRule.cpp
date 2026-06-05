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

#include <Rules/Dynamic/StrawmanDynamicRule.hpp>

#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>

#include <Functions/ArithmeticalFunctions/MulLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Plans/LogicalPlan.hpp>
#include "Operators/ProjectionLogicalOperator.hpp"

namespace NES
{

LogicalOperator straw(LogicalOperator op, const PlanStatistics& statistics)
{
    std::vector<LogicalOperator> newChildren;
    for (auto child: op.getChildren())
    {
        newChildren.emplace_back(straw(child, statistics));
    }

    if (auto projectionOp  = op.tryGetAs<ProjectionLogicalOperator>())
    {
        auto projections = std::vector<std::pair<Identifier, LogicalFunction>>();

        for (auto [field, function]: projectionOp.value()->getProjections())
        {
            if (function.getDataType() == DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE})
            {
                function = MulLogicalFunction{function, ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}};
            }
            projections.emplace_back(field.getLastName(), function);
        }
        return ProjectionLogicalOperator::create(newChildren.at(0), projections, ProjectionLogicalOperator::Asterisk{projectionOp.value()->hasAsterisk()});
    }
    return op.withChildren({newChildren});
}


LogicalPlan StrawmanDynamicRule::apply(LogicalPlan queryPlan, PlanStatistics statistics) const
{
    auto root = queryPlan.getRootOperators().at(0);

    auto newRoot = straw(root, statistics).withInferredSchema();

    queryPlan = queryPlan.withRootOperators({newRoot});

    return queryPlan;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan StrawmanDynamicRule::apply(LogicalPlan) const
{
    throw NotImplemented("use ::appl(LogicalPlan, PlanStatistics");
}



const std::type_info& StrawmanDynamicRule::getType()
{
    return typeid(StrawmanDynamicRule);
}

std::string_view StrawmanDynamicRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> StrawmanDynamicRule::dependsOn() const
{
    return {};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> StrawmanDynamicRule::requiredBy() const
{
    return {};
}

bool StrawmanDynamicRule::operator==(const StrawmanDynamicRule&) const
{
    return true;
}
}
