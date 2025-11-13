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
#include <string>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Model.hpp>

namespace NES::InferModel
{

/**
 * @brief Infer model operator
 */
class InferModelLogicalOperator : public OriginIdAssigner
{
    Nebuli::Inference::Model model;
    std::vector<LogicalFunction> inputFields;
    LogicalOperator child;
    Schema inputSchema;
    Schema outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;

public:
    [[nodiscard]] const Nebuli::Inference::Model& getModel() const { return model; }
    [[nodiscard]] const std::vector<LogicalFunction>& getInputFields() const { return inputFields; }
    InferModelLogicalOperator(NES::Nebuli::Inference::Model, std::vector<LogicalFunction> inputFields);
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const { return {child}; }
    [[nodiscard]] InferModelLogicalOperator withChildren(std::vector<LogicalOperator> children) const
    {
        PRECONDITION(children.size() == 1, "Expected exactly one child");
        auto copy = *this;
        copy.child = std::move(children.front());
        return copy;
    }
    [[nodiscard]] bool operator==(const InferModelLogicalOperator& rhs) const 
    {
        return model == rhs.model && inputFields == rhs.inputFields && child == rhs.child
            && rhs.getInputSchemas() == getInputSchemas() && rhs.getOutputSchema() == getOutputSchema();
    }
    [[nodiscard]] std::string_view getName() const noexcept { return NAME; }
    void serialize(SerializableOperator&) const ;
    [[nodiscard]] TraitSet getTraitSet() const { return {traitSet}; }
    [[nodiscard]] InferModelLogicalOperator withTraitSet(TraitSet traitSet) const ;
    [[nodiscard]] std::vector<Schema> getInputSchemas() const { return {inputSchema}; }
    [[nodiscard]] const Schema& getInputSchema() const { return inputSchema; }
    InferModelLogicalOperator setInputSchema(std::vector<Schema> inputSchemas) const
    {
        PRECONDITION(inputSchemas.size() == 1, "Expected exactly one schema");
        auto copy = *this;
        copy.inputSchema = std::move(inputSchemas.at(0));
        return copy;
    }
    [[nodiscard]] Schema getOutputSchema() const { return outputSchema; }
    InferModelLogicalOperator setOutputSchema(Schema outputSchema) const
    {
        auto copy = *this;
        copy.outputSchema = std::move(outputSchema);
        return copy;
    }
    [[nodiscard]] InferModelLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const ;
private:
    static constexpr std::string_view NAME = "InferModel";
    TraitSet traitSet;
};
}
