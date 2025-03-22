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

#include <Operators/LogicalOperator.hpp>

namespace NES
{

/// Is constructed during parsing. Stores the name of one logical source as a member.
/// In the LogicalSourceExpansionRule, we use the logical source name as input to the source catalog, to retrieve all (physical) source descriptors
/// configured for the specific logical source name. We then expand 1 SourceNameLogicalOperator to N SourceDescriptorLogicalOperators,
/// one SourceDescriptorLogicalOperator for each descriptor found in the source catalog with the logical source name as input.
class SourceNameLogicalOperator : public LogicalOperatorConcept
{
public:
    explicit SourceNameLogicalOperator(std::string logicalSourceName);
    explicit SourceNameLogicalOperator(std::string logicalSourceName, const Schema& schema);
    [[nodiscard]] std::string_view getName() const noexcept override;

    /// Returns the result schema of a source operator, which is defined by the source descriptor.
    bool inferSchema();

    [[nodiscard]] bool operator==(LogicalOperatorConcept const& rhs) const override;

    void inferInputOrigins();

    [[nodiscard]] Schema getSchema() const;
    void setSchema(const Schema& schema);

    [[nodiscard]] SerializableOperator serialize() const override;
    [[nodiscard]] std::string toString() const override;

    [[nodiscard]] std::string getLogicalSourceName() const;

    std::vector<LogicalOperator> getChildren() const override {return children;};
    void setChildren(std::vector<LogicalOperator> children) override {this->children = children;};
    Optimizer::TraitSet getTraitSet() const override { return {};};

    std::vector<std::vector<OriginId>> getInputOriginIds() const override { return {}; }
    std::vector<OriginId> getOutputOriginIds() const override { return {}; }

    std::vector<Schema> getInputSchemas() const override { return {schema}; };
    Schema getOutputSchema() const override { return schema; };

private:
    std::string logicalSourceName;
    std::vector<LogicalOperator> children;
    Schema schema;
};

}
