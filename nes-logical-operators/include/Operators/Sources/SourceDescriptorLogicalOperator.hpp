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

#include <Operators/OriginIdAssignmentOperator.hpp>
#include "Operators/UnaryLogicalOperator.hpp"
#include "Sources/SourceDescriptor.hpp"

namespace NES
{

/// Is constructed when we apply the LogicalSourceExpansionRule. Stores the Descriptor of a (physical) source as a member.
/// During parsing, we register (physical) source descriptors in the source catalog. Each currently must name exactly one logical source.
/// The logical source is then used as key to a multimap, with all descriptors that name the logical source as values.
/// In the LogicalSourceExpansionRule, we take the keys from SourceNameLogicalOperator operators, get all corresponding (physical) source
/// descriptors from the catalog, construct SourceDescriptorLogicalOperators from the descriptors and attach them to the query plan.
class SourceDescriptorLogicalOperator : public UnaryLogicalOperator, public OriginIdAssignmentOperator
{
public:
    explicit SourceDescriptorLogicalOperator(std::shared_ptr<Sources::SourceDescriptor>&& sourceDescriptor);
    explicit SourceDescriptorLogicalOperator(
        std::shared_ptr<Sources::SourceDescriptor>&& sourceDescriptor, OriginId originId);
    [[nodiscard]] std::string_view getName() const noexcept override;

    const Sources::SourceDescriptor& getSourceDescriptorRef() const;
    std::shared_ptr<Sources::SourceDescriptor> getSourceDescriptor() const;

    /// Returns the result schema of a source operator, which is defined by the source descriptor.
    bool inferSchema() override;

    [[nodiscard]] bool operator==(Operator const& rhs) const override;
    [[nodiscard]] bool isIdentical(const Operator& rhs) const override;

    std::shared_ptr<Operator> clone() const override;
    void inferInputOrigins() override;
    std::vector<OriginId> getOutputOriginIds() const override;

    [[nodiscard]] SerializableOperator serialize() const override;

    protected:
    [[nodiscard]] std::string toString() const override;

private:
    const std::shared_ptr<Sources::SourceDescriptor> sourceDescriptor;
};

}
