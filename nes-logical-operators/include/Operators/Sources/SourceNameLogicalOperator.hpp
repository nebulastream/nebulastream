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
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

/// Is constructed during SQL parsing. Stores the name of one logical source as a member.
/// In the LogicalSourceExpansionRule, we use the logical source name as input to the source catalog, to retrieve all (physical) source descriptors
/// configured for the specific logical source name. We then expand 1 SourceNameLogicalOperator to N SourceDescriptorLogicalOperators,
/// one SourceDescriptorLogicalOperator for each descriptor found in the source catalog with the logical source name as input.
class SourceNameLogicalOperator : public LogicalOperatorConcept
{
public:
    explicit SourceNameLogicalOperator(std::string logicalSourceName);
    explicit SourceNameLogicalOperator(std::string logicalSourceName, Schema schema);

    static void inferInputOrigins();

    [[nodiscard]] std::string getLogicalSourceName() const;

    [[nodiscard]] Schema getSchema() const;
    [[nodiscard]] LogicalOperator withSchema(const Schema& schema) const;


    [[nodiscard]] bool operator==(const LogicalOperatorConcept& rhs) const override;
    [[nodiscard]] SerializableOperator serialize() const override;

    [[nodiscard]] LogicalOperator withTraitSet(TraitSet traitSet) const override;
    [[nodiscard]] TraitSet getTraitSet() const override;

    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const override;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const override;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const override;
    [[nodiscard]] Schema getOutputSchema() const override;

    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override;
    [[nodiscard]] LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const override;
    [[nodiscard]] LogicalOperator withOutputOriginIds(std::vector<OriginId> ids) const override;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const override;

private:
    static constexpr std::string_view NAME = "Source";
    std::string logicalSourceName;

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema schema, inputSchema, outputSchema;
    std::vector<std::vector<OriginId>> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};

}
