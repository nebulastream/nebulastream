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

#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>

namespace NES
{

/// Is constructed during parsing. Stores the name of one logical source as a member.
/// In the LogicalSourceExpansionRule, we use the logical source name as input to the source catalog, to retrieve all (physical) source descriptors
/// configured for the specific logical source name. We then expand 1 SourceNameLogicalOperator to N SourceDescriptorLogicalOperators,
/// one SourceDescriptorLogicalOperator for each descriptor found in the source catalog with the logical source name as input.
class SourceNameLogicalOperator : public LogicalUnaryOperator
{
public:
    explicit SourceNameLogicalOperator(std::string logicalSourceName, OperatorId id);
    explicit SourceNameLogicalOperator(std::string logicalSourceName, std::shared_ptr<Schema> schema, OperatorId id);

    /// Returns the result schema of a source operator, which is defined by the source descriptor.
    bool inferSchema() override;

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    void inferStringSignature() override;
    OperatorPtr copy() override;
    void inferInputOrigins() override;

    [[nodiscard]] std::string getLogicalSourceName() const;
    [[nodiscard]] std::shared_ptr<Schema> getSchema() const;
    void setSchema(std::shared_ptr<Schema> schema);

protected:
    [[nodiscard]] std::string toString() const override;

private:
    std::string logicalSourceName;
    std::shared_ptr<Schema> schema;
};

}
