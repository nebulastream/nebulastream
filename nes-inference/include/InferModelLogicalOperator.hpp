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
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>
#include <Model.hpp>

namespace NES::InferModel
{

class InferModelLogicalOperator final : public OriginIdAssigner
{
public:
    explicit InferModelLogicalOperator(NES::Nebuli::Inference::Model, std::vector<LogicalFunction> inputFields);

    [[nodiscard]] const Nebuli::Inference::Model& getModel() const;
    [[nodiscard]] const std::vector<LogicalFunction>& getInputFields() const;

    [[nodiscard]] bool operator==(const InferModelLogicalOperator& rhs) const;

    void serialize(SerializableOperator& serializableOperator) const;

    [[nodiscard]] InferModelLogicalOperator withTraitSet(TraitSet traitSet) const ;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] InferModelLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] InferModelLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const ;

private:
    static constexpr std::string_view NAME = "INFERENCEMODEL";
    Nebuli::Inference::Model model;
    std::vector<LogicalFunction> inputFields;

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
};

static_assert(LogicalOperatorConcept<InferModelLogicalOperator>);
}
