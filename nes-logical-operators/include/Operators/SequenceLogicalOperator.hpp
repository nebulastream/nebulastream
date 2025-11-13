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
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

class SequenceLogicalOperator : public OriginIdAssigner
{
public:
    explicit SequenceLogicalOperator();

    [[nodiscard]] bool operator==(const SequenceLogicalOperator& rhs) const;
    void serialize(SerializableOperator&) const;

    [[nodiscard]] TraitSet getTraitSet() const;
    [[nodiscard]] SequenceLogicalOperator withTraitSet(TraitSet traitSet) const;

    [[nodiscard]] SequenceLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] SequenceLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

    /// Set the schemas directly without inference used for operator registration
    [[nodiscard]] SequenceLogicalOperator setInputSchemas(std::vector<Schema> inputSchemas) const;
    [[nodiscard]] SequenceLogicalOperator setOutputSchema(const Schema& outputSchema) const;

private:
    static constexpr std::string_view NAME = "Sequence";

    std::vector<LogicalOperator> children;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
    TraitSet traitSet;
};


}
