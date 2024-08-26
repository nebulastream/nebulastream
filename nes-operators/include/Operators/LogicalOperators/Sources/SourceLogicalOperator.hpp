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

/**
 * @brief Node representing logical source operator
 */
class SourceLogicalOperator : public LogicalUnaryOperator, public OriginIdAssignmentOperator
{
public:
    explicit SourceLogicalOperator(std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor, OperatorId id);
    explicit SourceLogicalOperator(std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor, OperatorId id, OriginId originId);

    std::unique_ptr<Sources::SourceDescriptor> getSourceDescriptor();

    void setSourceDescriptor(std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor);

    /// Returns the result schema of a source operator, which is defined by the source descriptor.
    bool inferSchema() override;

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    void inferStringSignature() override;
    OperatorPtr copy() override;
    void setProjectSchema(SchemaPtr schema);
    void inferInputOrigins() override;
    std::vector<OriginId> getOutputOriginIds() const override;

private:
    std::unique_ptr<Sources::SourceDescriptor> sourceDescriptor;
    SchemaPtr projectSchema;
};

using SourceLogicalOperatorPtr = std::shared_ptr<SourceLogicalOperator>;
} /// namespace NES
