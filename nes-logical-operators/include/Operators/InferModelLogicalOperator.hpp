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
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Reflection.hpp>
#include <Model.hpp>

namespace NES
{

/// Operator that holds a compiled inference model. Input/output field names come from the model itself.
class InferModelLogicalOperator
{
public:
    explicit InferModelLogicalOperator(Inference::Model model);

    [[nodiscard]] const Inference::Model& getModel() const;
    [[nodiscard]] Inference::Model& getModel();

    /// Returns model input field names (from CREATE MODEL INPUT)
    [[nodiscard]] std::vector<std::string> getInputFieldNames() const;

    [[nodiscard]] bool operator==(const InferModelLogicalOperator& rhs) const;

    [[nodiscard]] InferModelLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] InferModelLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] InferModelLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

private:
    static constexpr std::string_view NAME = "InferModel";
    Inference::Model model;

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
struct ReflectedModelOutput
{
    std::string fieldName;
    DataType::Type dataType;
};

struct ReflectedInferModelLogicalOperator
{
    std::string functionName;
    std::vector<ReflectedModelOutput> modelOutputs;
    std::vector<ReflectedModelOutput> modelInputs;
    std::string bytecodeBase64;
    std::vector<size_t> inputShape;
    std::vector<size_t> outputShape;
    size_t inputSizeInBytes = 0;
    size_t outputSizeInBytes = 0;
};
}
