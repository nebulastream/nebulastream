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
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <Windowing/WindowMetaData.hpp>

namespace NES
{
class SerializableOperator;

class JoinLogicalOperator final : public OriginIdAssigner
{
public:
    enum class JoinType : uint8_t
    {
        INNER_JOIN,
        CARTESIAN_PRODUCT
    };

    /// Wire shape: join predicate + opaque window-type blob (reflected/
    /// unreflected via `reflectWindowType`/`unreflectWindowType` in the .cpp) +
    /// join type enum. `wire()` / `fromWire` are defined in the .cpp because
    /// both ends need `WindowTypeReflection.hpp`.
    struct Wire
    {
        LogicalFunction joinFunction;
        Reflected windowType;
        JoinType joinType = JoinType::INNER_JOIN;
    };
    [[nodiscard]] Wire wire() const;
    [[nodiscard]] static JoinLogicalOperator fromWire(Wire wire, const ReflectionContext& context);

    explicit JoinLogicalOperator(LogicalFunction joinFunction, std::shared_ptr<Windowing::WindowType> windowType, JoinType joinType);

    [[nodiscard]] LogicalFunction getJoinFunction() const;
    [[nodiscard]] Schema getLeftSchema() const;
    [[nodiscard]] Schema getRightSchema() const;
    [[nodiscard]] std::shared_ptr<Windowing::WindowType> getWindowType() const;
    [[nodiscard]] std::string getWindowStartFieldName() const;
    [[nodiscard]] std::string getWindowEndFieldName() const;
    [[nodiscard]] const WindowMetaData& getWindowMetaData() const;


    [[nodiscard]] bool operator==(const JoinLogicalOperator& rhs) const;

    [[nodiscard]] JoinLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] JoinLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] JoinLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

private:
    static constexpr std::string_view NAME = "Join";
    LogicalFunction joinFunction;
    std::shared_ptr<Windowing::WindowType> windowType;
    WindowMetaData windowMetaData;
    JoinType joinType;

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema leftInputSchema, rightInputSchema, outputSchema;
};

static_assert(LogicalOperatorConcept<JoinLogicalOperator>);
}
