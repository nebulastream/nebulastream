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
#include <Model.hpp>

namespace NES::InferModel
{

/**
 * @brief Infer model operator
 */
class LogicalInferModelOperator : public LogicalOperatorConcept
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
    LogicalInferModelOperator(NES::Nebuli::Inference::Model, std::vector<LogicalFunction> inputFields);
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const override { return {child}; }
    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const override
    {
        PRECONDITION(children.size() == 1, "Expected exactly one child");
        auto copy = *this;
        copy.child = std::move(children.front());
        return copy;
    }
    [[nodiscard]] bool operator==(const LogicalOperatorConcept& rhs) const override
    {
        if (const auto* casted = dynamic_cast<const LogicalInferModelOperator*>(&rhs))
        {
            return model == casted->model && inputFields == casted->inputFields && child == casted->child
                && casted->getInputSchemas() == getInputSchemas() && casted->getOutputSchema() == getOutputSchema()
                && casted->getInputOriginIds() == getInputOriginIds() && casted->getOutputOriginIds() == getOutputOriginIds();
        }
        return false;
    }
    [[nodiscard]] std::string_view getName() const noexcept override { return "InferenceModel"; }
    [[nodiscard]] SerializableOperator serialize() const override;
    [[nodiscard]] TraitSet getTraitSet() const override { return {}; }
    [[nodiscard]] std::vector<Schema> getInputSchemas() const override { return {inputSchema}; }
    [[nodiscard]] const Schema& getInputSchema() const { return inputSchema; }
    LogicalInferModelOperator setInputSchema(std::vector<Schema> inputSchemas) const
    {
        PRECONDITION(inputSchemas.size() == 1, "Expected exactly one schema");
        auto copy = *this;
        copy.inputSchema = std::move(inputSchemas.at(0));
        return copy;
    }
    [[nodiscard]] Schema getOutputSchema() const override { return outputSchema; }
    LogicalInferModelOperator setOutputSchema(Schema outputSchema) const
    {
        auto copy = *this;
        copy.outputSchema = std::move(outputSchema);
        return copy;
    }
    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override { return {inputOriginIds}; }
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override { return outputOriginIds; }
    [[nodiscard]] LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>> originIds) const override
    {
        PRECONDITION(originIds.size() == 1, "Expected exactly one set of origin ids");
        auto copy = *this;
        copy.inputOriginIds = std::move(originIds.front());
        return copy;
    }

    [[nodiscard]] LogicalOperator withOutputOriginIds(std::vector<OriginId> originIds) const override
    {
        auto copy = *this;
        copy.outputOriginIds = std::move(originIds);
        return copy;
    }
    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const override;
};


}
