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

#include <Serialization/ReflectedOperator.hpp>

#include <optional>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Operators/LogicalOperatorFwd.hpp>

#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

ReflectedPlan::ReflectedPlan(std::unordered_map<OperatorId, ReflectedOperator> operators) : reflectedOperators(std::move(operators))
{
    this->operatorsToChildren = this->reflectedOperators
        | std::views::transform([](const auto& pair) { return std::pair{pair.first, pair.second.childrenIds}; })
        | std::ranges::to<std::unordered_map>();

    this->traitSets = this->reflectedOperators
        | std::views::transform([](const auto& pair) { return std::pair{pair.first, pair.second.traitSet}; })
        | std::ranges::to<std::unordered_map>();
}

TraitSet ReflectedPlan::getTraitSetFor(OperatorId operatorId, const ReflectionContext&) const
{
    if (const auto foundTraitSet = traitSets.find(operatorId); foundTraitSet != traitSets.end())
    {
        return foundTraitSet->second;
    }
    throw CannotDeserialize("Could not find trait set for operator {}", operatorId);
}

std::optional<LogicalOperator> ReflectedPlan::getOperator(OperatorId operatorId, const ReflectionContext& context)
{
    if (const auto& foundOperator = builtOperators.find(operatorId); foundOperator != builtOperators.end())
    {
        return foundOperator->second;
    }
    if (const auto& foundReflected = reflectedOperators.find(operatorId); foundReflected != reflectedOperators.end())
    {
        auto createdOpt
            = LogicalOperatorRegistry::instance().unreflect(foundReflected->second.type, foundReflected->second.config, context);
        if (!createdOpt.has_value())
        {
            throw CannotDeserialize("Could not create operator of type {}", foundReflected->second.type);
        }
        auto created = std::move(createdOpt).value();
        created = created.withTraitSet(foundReflected->second.traitSet);
        created = created.withOperatorId(OperatorId{operatorId});
        builtOperators.emplace(operatorId, created);
        return created;
    }
    return std::nullopt;
}

std::vector<LogicalOperator> ReflectedPlan::getChildrenFor(OperatorId operatorId, const ReflectionContext& context)
{
    if (const auto foundChildrenIds = operatorsToChildren.find(operatorId); foundChildrenIds != operatorsToChildren.end())
    {
        std::vector<LogicalOperator> result;
        for (auto childId : foundChildrenIds->second)
        {
            if (auto created = getOperator(childId, context))
            {
                result.push_back(std::move(created).value());
            }
            else
            {
                throw CannotDeserialize("Could not find operator with id {} in operators list", childId);
            }
        }
        return result;
    }
    throw CannotDeserialize("Could not find children of operator {}", operatorId);
}
}
