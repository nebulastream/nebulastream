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
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <Model.hpp>

namespace NES
{

/// ML inference operator that evaluates a compiled model over selected input fields
/// and appends the model's output fields to the output schema.
class InferModelLogicalOperator
{
public:
    explicit InferModelLogicalOperator(Model model, std::vector<std::string> inputFieldNames);

    [[nodiscard]] Model getModel() const;
    [[nodiscard]] std::vector<std::string> getInputFieldNames() const;

    [[nodiscard]] bool operator==(const InferModelLogicalOperator& rhs) const;

    [[nodiscard]] InferModelLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] InferModelLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string_view getName() const noexcept;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] InferModelLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

private:
    static constexpr std::string_view NAME = "InferModel";
    Model model;
    std::vector<std::string> inputFieldNames;

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
};

template <>
struct Reflector<InferModelLogicalOperator>
{
    Reflected operator()(const InferModelLogicalOperator& op) const;
};

template <>
struct Unreflector<InferModelLogicalOperator>
{
    InferModelLogicalOperator operator()(const Reflected& rfl) const;
};

static_assert(LogicalOperatorConcept<InferModelLogicalOperator>);

}

namespace NES::detail
{
struct ReflectedInferModelLogicalOperator
{
    std::optional<Reflected> model;
    std::optional<std::vector<std::string>> inputFieldNames;
};
}
