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
#include <LogicalOperators/Operator.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/OriginIdAssignerTrait.hpp>

namespace NES::Logical
{

/// Is constructed when we apply the SourceExpansionRule. Stores the Descriptor of a (physical) source as a member.
/// During parsing, we register (physical) source descriptors in the source catalog. Each currently must name exactly one logical source.
/// The logical source is then used as key to a multimap, with all descriptors that name the logical source as values.
/// In the SourceExpansionRule, we take the keys from SourceNameOperator operators, get all corresponding (physical) source
/// descriptors from the catalog, construct SourceDescriptorOperators from the descriptors and attach them to the query plan.
class SourceDescriptorOperator final : public OperatorConcept
{
public:
    explicit SourceDescriptorOperator(std::shared_ptr<Sources::SourceDescriptor>&& sourceDescriptor);

    /// Operator specific member
    [[nodiscard]] std::shared_ptr<Sources::SourceDescriptor> getSourceDescriptor() const;
    [[nodiscard]] Sources::SourceDescriptor& getSourceDescriptorRef() const;

    /// OperatorConcept member
    [[nodiscard]] bool operator==(const OperatorConcept& rhs) const override;
    [[nodiscard]] NES::SerializableOperator serialize() const override;

    [[nodiscard]] Optimizer::TraitSet getTraitSet() const override;

    [[nodiscard]] Operator withChildren(std::vector<Operator> children) const override;
    [[nodiscard]] std::vector<Operator> getChildren() const override;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const override;
    [[nodiscard]] Schema getOutputSchema() const override;

    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override;
    [[nodiscard]] Operator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const override;
    [[nodiscard]] Operator withOutputOriginIds(std::vector<OriginId> ids) const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    [[nodiscard]] Operator withInferredSchema(std::vector<Schema> inputSchemas) const override;


private:
    /// Operator specific member
    static constexpr std::string_view NAME = "Source";
    const std::shared_ptr<Sources::SourceDescriptor> sourceDescriptor;
    Optimizer::OriginIdAssignerTrait originIdTrait;

    /// OperatorConcept member
    std::vector<Operator> children;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};
}
