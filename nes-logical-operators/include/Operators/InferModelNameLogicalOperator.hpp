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
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Placeholder operator that holds only the model name and input fields.
/// The ModelInferenceCompilationRule resolves this to an InferModelLogicalOperator with a compiled Model.
class InferModelNameLogicalOperator
{
public:
    InferModelNameLogicalOperator(std::string modelName, std::vector<LogicalFunction> inputFields);

    [[nodiscard]] const std::string& getModelName() const;
    [[nodiscard]] const std::vector<LogicalFunction>& getInputFields() const;

    [[nodiscard]] bool operator==(const InferModelNameLogicalOperator& rhs) const;

    [[nodiscard]] InferModelNameLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] InferModelNameLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] InferModelNameLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

private:
    static constexpr std::string_view NAME = "InferModelName";
    std::string modelName;
    std::vector<LogicalFunction> inputFields;

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
};

template <>
struct Reflector<InferModelNameLogicalOperator>
{
    Reflected operator()(const InferModelNameLogicalOperator& op) const;
};

template <>
struct Unreflector<InferModelNameLogicalOperator>
{
    InferModelNameLogicalOperator operator()(const Reflected& rfl) const;
};

static_assert(LogicalOperatorConcept<InferModelNameLogicalOperator>);
}

namespace NES::detail
{
struct ReflectedInferModelNameLogicalOperator
{
    std::string modelName;
    std::vector<LogicalFunction> inputFields;
};
}
