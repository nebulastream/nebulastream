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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/LegacySchema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <Windowing/WindowMetaData.hpp>

namespace NES
{
class SerializableOperator;

class JoinLogicalOperator final : public OriginIdAssigner, public ManagedByOperator
{
public:
    enum class JoinType : uint8_t
    {
        INNER_JOIN,
        CARTESIAN_PRODUCT
    };

    explicit JoinLogicalOperator(
        WeakLogicalOperator self, LogicalFunction joinFunction, std::shared_ptr<Windowing::WindowType> windowType, JoinType joinType);

    [[nodiscard]] LogicalFunction getJoinFunction() const;
    [[nodiscard]] LegacySchema getLeftSchema() const;
    [[nodiscard]] LegacySchema getRightSchema() const;
    [[nodiscard]] std::shared_ptr<Windowing::WindowType> getWindowType() const;
    [[nodiscard]] std::string getWindowStartFieldName() const;
    [[nodiscard]] std::string getWindowEndFieldName() const;
    [[nodiscard]] const WindowMetaData& getWindowMetaData() const;


    [[nodiscard]] bool operator==(const JoinLogicalOperator& rhs) const;

    [[nodiscard]] JoinLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] JoinLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<LegacySchema> getInputSchemas() const;
    [[nodiscard]] LegacySchema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] JoinLogicalOperator withInferredSchema(std::vector<LegacySchema> inputSchemas) const;

private:
    static constexpr std::string_view NAME = "Join";
    LogicalFunction joinFunction;
    std::shared_ptr<Windowing::WindowType> windowType;
    WindowMetaData windowMetaData;
    JoinType joinType;

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    LegacySchema leftInputSchema, rightInputSchema, outputSchema;

    friend Reflector<TypedLogicalOperator<JoinLogicalOperator>>;
};

template <>
struct Reflector<TypedLogicalOperator<JoinLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<JoinLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<JoinLogicalOperator>>
{
    TypedLogicalOperator<JoinLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<JoinLogicalOperator>);
}

namespace NES::detail
{
struct ReflectedJoinLogicalOperator
{
    LogicalFunction joinFunction;
    Reflected windowType;
    JoinLogicalOperator::JoinType joinType = JoinLogicalOperator::JoinType::INNER_JOIN;
};
}
