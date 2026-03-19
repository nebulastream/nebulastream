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
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Reflection/ReflectionCore.hpp>

#include <Serialization/OperatorMapping.hpp>

namespace NES
{
struct ReflectedOperator
{
    std::string type;
    OperatorId operatorId;
    std::vector<OperatorId> childrenIds;
    Reflected config;
    TraitSet traitSet;
};

class ReflectedPlan : public OperatorMapping
{
public:
    explicit ReflectedPlan(std::unordered_map<OperatorId, ReflectedOperator> operators);

    ///Throws CannotDeserialize if no entry was found for operator
    [[nodiscard]] std::vector<LogicalOperator> getChildrenFor(OperatorId operatorId, const ReflectionContext& context);
    [[nodiscard]] TraitSet getTraitSetFor(OperatorId operatorId, const ReflectionContext& context) const;
    [[nodiscard]] std::optional<LogicalOperator> getOperator(OperatorId operatorId, const ReflectionContext& context) override;

private:
    friend class QueryPlanSerializationUtil;

    std::unordered_map<OperatorId, ReflectedOperator> reflectedOperators;
    std::unordered_map<OperatorId, std::vector<OperatorId>> operatorsToChildren;
    std::unordered_map<OperatorId, LogicalOperator> builtOperators;
    std::unordered_map<OperatorId, TraitSet> traitSets;
};
}
