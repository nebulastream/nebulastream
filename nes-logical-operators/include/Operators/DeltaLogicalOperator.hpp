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
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Delta operator computes the difference between consecutive values for specified fields.
/// The first tuple is dropped since there is no previous value.
/// All original fields are passed through; delta fields are either added (with alias) or overwrite the original field (without alias).
class DeltaLogicalOperator
{
public:
    /// Each delta expression is a (alias, field access function) pair.
    /// A new field with the alias name is appended to the output schema.
    using DeltaExpression = std::pair<FieldIdentifier, LogicalFunction>;

    explicit DeltaLogicalOperator(std::vector<DeltaExpression> deltaExpressions);

    [[nodiscard]] const std::vector<DeltaExpression>& getDeltaExpressions() const;

    [[nodiscard]] bool operator==(const DeltaLogicalOperator& rhs) const;

    [[nodiscard]] DeltaLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] DeltaLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] DeltaLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

private:
    /// Converts a numeric data type to its signed counterpart for the delta result.
    static DataType toSignedType(DataType inputType);

    static constexpr std::string_view NAME = "Delta";
    std::vector<DeltaExpression> deltaExpressions;

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;

    friend Reflector<DeltaLogicalOperator>;
};

template <>
struct Reflector<DeltaLogicalOperator>
{
    Reflected operator()(const DeltaLogicalOperator& op) const;
};

template <>
struct Unreflector<DeltaLogicalOperator>
{
    DeltaLogicalOperator operator()(const Reflected& reflected) const;
};

static_assert(LogicalOperatorConcept<DeltaLogicalOperator>);

}

namespace NES::detail
{
struct ReflectedDeltaLogicalOperator
{
    std::vector<std::pair<std::string, std::optional<LogicalFunction>>> deltaExpressions;
};
}
