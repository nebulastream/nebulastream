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

#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>

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
    void serialize(SerializableOperator&) const;

    [[nodiscard]] DeltaLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] DeltaLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] DeltaLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<std::string> DELTA_EXPRESSIONS_NAME{
            "deltaExpressionsName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(DELTA_EXPRESSIONS_NAME, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(DELTA_EXPRESSIONS_NAME);
    };

private:
    /// Converts a numeric data type to its signed counterpart for the delta result.
    static DataType toSignedType(DataType inputType);

    static constexpr std::string_view NAME = "Delta";
    std::vector<DeltaExpression> deltaExpressions;

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
};

static_assert(LogicalOperatorConcept<DeltaLogicalOperator>);

}
