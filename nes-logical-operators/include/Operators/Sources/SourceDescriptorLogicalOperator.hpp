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
#include <Sources/SourceDescriptor.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/OriginIdTrait.hpp>

namespace NES
{

/// Is constructed when we apply the LogicalSourceExpansionRule. Stores the Descriptor of a (physical) source as a member.
/// During parsing, we register (physical) source descriptors in the source catalog. Each currently must name exactly one logical source.
/// The logical source is then used as key to a multimap, with all descriptors that name the logical source as values.
/// In the LogicalSourceExpansionRule, we take the keys from SourceNameLogicalOperator operators, get all corresponding (physical) source
/// descriptors from the catalog, construct SourceDescriptorLogicalOperators from the descriptors and attach them to the query plan.
class SourceDescriptorLogicalOperator final : public LogicalOperatorConcept
{
public:
    explicit SourceDescriptorLogicalOperator(std::shared_ptr<Sources::SourceDescriptor>&& sourceDescriptor);
    [[nodiscard]] std::string_view getName() const noexcept override;

    [[nodiscard]] std::shared_ptr<Sources::SourceDescriptor> getSourceDescriptor() const;
    [[nodiscard]] Sources::SourceDescriptor& getSourceDescriptorRef() const;

    /// Returns the result schema of a source operator, which is defined by the source descriptor.
    bool inferSchema();

    [[nodiscard]] bool operator==(LogicalOperatorConcept const& rhs) const override;

    Optimizer::OriginIdTrait originIdTrait;

    [[nodiscard]] SerializableOperator serialize() const override;

    [[nodiscard]] std::string toString() const override;

    virtual std::vector<LogicalOperator> getChildren() const override
    {
        return children;
    }

    [[nodiscard]] Optimizer::TraitSet getTraitSet() const override
    {
        return {};
    }

    void setChildren(std::vector<LogicalOperator> children) override
    {
        this->children = children;
    }

    std::vector<std::vector<OriginId>> getInputOriginIds() const override { return {}; }
    std::vector<OriginId> getOutputOriginIds() const override { return {}; }

    std::vector<Schema> getInputSchemas() const override { return {inputSchema}; };
    Schema getOutputSchema() const override { return outputSchema; };


    void setInputOriginIds(std::vector<std::vector<OriginId>> ids) override
    {
        inputOriginIds = ids;
    }

    void setOutputOriginIds(std::vector<OriginId> ids) override
    {
        outputOriginIds = ids;
    }
private:
    std::vector<LogicalOperator> children;
    Schema inputSchema, outputSchema;

    static constexpr std::string_view NAME = "SourceDescriptor";
    const std::shared_ptr<Sources::SourceDescriptor> sourceDescriptor;
    std::vector<std::vector<OriginId>> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};

}
