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
#include <Operators/LogicalOperator.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/OriginIdAssignerTrait.hpp>

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

    /// Operator specific member
    [[nodiscard]] std::shared_ptr<Sources::SourceDescriptor> getSourceDescriptor() const;
    [[nodiscard]] Sources::SourceDescriptor& getSourceDescriptorRef() const;

    /// LogicalOperatorConcept member
    [[nodiscard]] bool operator==(LogicalOperatorConcept const& rhs) const override;
    [[nodiscard]] SerializableOperator serialize() const override;

    [[nodiscard]] Optimizer::TraitSet getTraitSet() const override;

    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const override;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const override;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const override;
    [[nodiscard]] Schema getOutputSchema() const override;

    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override;
    void setInputOriginIds(std::vector<std::vector<OriginId>> ids) override;
    void setOutputOriginIds(std::vector<OriginId> ids) override;

    [[nodiscard]] std::string toString() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    [[nodiscard]] LogicalOperator withInferredSchema(Schema inputSchema) const override;


private:
    /// Operator specific member
    static constexpr std::string_view NAME = "Source";
    const std::shared_ptr<Sources::SourceDescriptor> sourceDescriptor;

    /// LogicalOperatorConcept member
    Optimizer::OriginIdAssignerTrait originIdTrait;
    std::vector<LogicalOperator> children;
    Schema inputSchema, outputSchema;
    std::vector<std::vector<OriginId>> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};

}
