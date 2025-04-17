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


#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Identifiers/Identifiers.hpp>

namespace NES
{
/// @brief Logical Binary operator, defines two output schemas
class BinaryLogicalOperator : public LogicalOperator
{
public:
    explicit BinaryLogicalOperator(OperatorId id);

    bool inferSchema() override;
    void inferInputOrigins() override;

    /// @brief Get all left input operators.
    /// @return std::vector<std::shared_ptr<Operator>>
    std::vector<std::shared_ptr<Operator>> getLeftOperators() const;

    /// @brief Get all right input operators.
    /// @return std::vector<std::shared_ptr<Operator>>
    std::vector<std::shared_ptr<Operator>> getRightOperators() const;

    void setRightInputSchema(std::shared_ptr<Schema> schema)
    {
        rightInputSchema = schema;
    }
    std::shared_ptr<Schema> getRightInputSchema() const
    {
        return rightInputSchema;
    }

    void setLeftInputSchema(std::shared_ptr<Schema> schema)
    {
        leftInputSchema = schema;
    }
    std::shared_ptr<Schema> getLeftInputSchema() const
    {
        return leftInputSchema;
    }

    void setLeftInputOriginIds(std::vector<OriginId> originIds)
    {
        leftInputOriginIds = originIds;
    }

    std::vector<OriginId> getLeftInputOriginIds() const
    {
        return leftInputOriginIds;
    }

    void setRightInputOriginIds(std::vector<OriginId> originIds)
    {
        rightInputOriginIds = originIds;
    }
    std::vector<OriginId> getRightInputOriginIds() const
    {
        return rightInputOriginIds;
    }

    std::vector<OriginId> getOutputOriginIds() const override
    {
        std::vector<OriginId> outputOriginIds = leftInputOriginIds;
        outputOriginIds.insert(outputOriginIds.end(), rightInputOriginIds.begin(), rightInputOriginIds.end());
        return outputOriginIds;
    }

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
