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


#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>

namespace NES
{
/// @brief Logical Binary operator, defines two output schemas
class BinaryLogicalOperator : public LogicalOperator
{
public:
    explicit BinaryLogicalOperator();

    bool inferSchema() override;
    void inferInputOrigins() override;

    /// @brief Get all left input operators.
    /// @return std::vector<std::shared_ptr<Operator>>
    std::vector<std::shared_ptr<Operator>> getLeftOperators() const;

    /// @brief Get all right input operators.
    /// @return std::vector<std::shared_ptr<Operator>>
    std::vector<std::shared_ptr<Operator>> getRightOperators() const;

    void setRightInputSchema(std::shared_ptr<Schema> schema);
    std::shared_ptr<Schema> getRightInputSchema() const;
    void setOutputSchema(std::shared_ptr<Schema> outputSchema) override;
    std::shared_ptr<Schema> getOutputSchema() const override;
    void setLeftInputSchema(std::shared_ptr<Schema> schema);
    std::shared_ptr<Schema> getLeftInputSchema() const;

    void setLeftInputOriginIds(std::vector<OriginId> originIds);
    std::vector<OriginId> getLeftInputOriginIds() const;
    void setRightInputOriginIds(std::vector<OriginId> originIds);
    std::vector<OriginId> getRightInputOriginIds() const;
    std::vector<OriginId> getOutputOriginIds() const override;
    std::vector<OriginId> getAllInputOriginIds();

protected:
    std::shared_ptr<Schema> leftInputSchema;
    std::shared_ptr<Schema> rightInputSchema;
    std::vector<std::shared_ptr<Schema>> distinctSchemas;

    std::vector<OriginId> leftInputOriginIds;
    std::vector<OriginId> rightInputOriginIds;

private:
    std::vector<std::shared_ptr<Operator>> getOperatorsBySchema(const std::shared_ptr<Schema>& schema) const;
};
}
