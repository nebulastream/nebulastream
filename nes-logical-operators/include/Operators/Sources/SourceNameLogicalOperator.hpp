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


#include "Operators/UnaryLogicalOperator.hpp"

namespace NES
{

/// Is constructed during parsing. Stores the name of one logical source as a member.
/// In the LogicalSourceExpansionRule, we use the logical source name as input to the source catalog, to retrieve all (physical) source descriptors
/// configured for the specific logical source name. We then expand 1 SourceNameLogicalOperator to N SourceDescriptorLogicalOperators,
/// one SourceDescriptorLogicalOperator for each descriptor found in the source catalog with the logical source name as input.
class SourceNameLogicalOperator : public UnaryLogicalOperator
{
public:
    explicit SourceNameLogicalOperator(std::string logicalSourceName);
    explicit SourceNameLogicalOperator(std::string logicalSourceName, std::shared_ptr<Schema> schema);

    /// Returns the result schema of a source operator, which is defined by the source descriptor.
    bool inferSchema() override;

    [[nodiscard]] bool operator==(Operator const& rhs) const override;
    [[nodiscard]] bool isIdentical(const Operator& rhs) const override;

    std::shared_ptr<Operator> clone() const override;
    void inferInputOrigins() override;

    [[nodiscard]] std::string getLogicalSourceName() const;
    [[nodiscard]] std::shared_ptr<Schema> getSchema() const;
    void setSchema(std::shared_ptr<Schema> schema);

    [[nodiscard]]  SerializableOperator serialize() const override;

protected:
    [[nodiscard]] std::string toString() const override;

private:
    std::string logicalSourceName;
    std::shared_ptr<Schema> schema;
};

}
