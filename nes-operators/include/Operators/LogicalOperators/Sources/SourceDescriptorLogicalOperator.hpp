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
#include <Nodes/Node.hpp>
#include <Operators/AbstractOperators/OriginIdAssignmentOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Operators/Operator.hpp>
#include <Identifiers/Identifiers.hpp>

namespace NES
{

/// Is constructed when we apply the LogicalSourceExpansionRule. Stores the Descriptor of a (physical) source as a member.
/// During parsing, we register (physical) source descriptors in the source catalog. Each currently must name exactly one logical source.
/// The logical source is then used as key to a multimap, with all descriptors that name the logical source as values.
/// In the LogicalSourceExpansionRule, we take the keys from SourceNameLogicalOperator operators, get all corresponding (physical) source
/// descriptors from the catalog, construct SourceDescriptorLogicalOperators from the descriptors and attach them to the query plan.
class SourceDescriptorLogicalOperator : public LogicalUnaryOperator, public OriginIdAssignmentOperator
{
public:
    explicit SourceDescriptorLogicalOperator(const Sources::SourceDescriptor& sourceDescriptor, OperatorId id);

    explicit SourceDescriptorLogicalOperator(
        Sources::SourceDescriptor sourceDescriptor, OperatorId id, OriginId originId);

    Sources::SourceDescriptor getSourceDescriptor() const;

    /// Returns the result schema of a source operator, which is defined by the source descriptor.
    bool inferSchema() override;

    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;
    [[nodiscard]] bool isIdentical(const std::shared_ptr<Node>& rhs) const override;
    void inferStringSignature() override;
    std::shared_ptr<Operator> copy() override;
    void inferInputOrigins() override;
    std::vector<OriginId> getOutputOriginIds() const override;

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;

private:
    Sources::SourceDescriptor sourceDescriptor;
};

}
