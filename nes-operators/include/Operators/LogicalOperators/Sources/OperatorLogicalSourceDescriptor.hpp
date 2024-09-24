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

#include <Operators/AbstractOperators/OriginIdAssignmentOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>

namespace NES
{

/// Is constructed during parsing and represents a logical source as an operator node in the query plan.
class OperatorLogicalSourceDescriptor : public LogicalUnaryOperator, public OriginIdAssignmentOperator
{
public:
    explicit OperatorLogicalSourceDescriptor(std::unique_ptr<Sources::DescriptorSource>&& DescriptorSource, OperatorId id);
    explicit OperatorLogicalSourceDescriptor(
        std::unique_ptr<Sources::DescriptorSource>&& DescriptorSource, OperatorId id, OriginId originId);

    const Sources::DescriptorSource& getDescriptorSourceRef() const;

    /// Returns the result schema of a source operator, which is defined by the source descriptor.
    bool inferSchema() override;

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    void inferStringSignature() override;
    OperatorPtr copy() override;
    void inferInputOrigins() override;
    std::vector<OriginId> getOutputOriginIds() const override;

private:
    const std::unique_ptr<Sources::DescriptorSource> descriptorSource;
};

}
