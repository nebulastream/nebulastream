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
#include <DataTypes/Schema.hpp>
#include <Operators/Operator.hpp>

namespace NES
{

/**
 * @brief A unary operator with one input operator, it has exactly one input operator.
 * This virtually inheritances for Operator
 * https://en.wikipedia.org/wiki/Virtual_inheritance
 */
class UnaryOperator : public virtual Operator
{
public:
    explicit UnaryOperator(OperatorId id);
    ~UnaryOperator() noexcept override = default;

    /**
   * @brief get the input schema of this operator
   * @return Schema
   */
    Schema getInputSchema() const;

    /**
     * @brief set the input schema of this operator
     * @param inputSchema
    */
    void setInputSchema(Schema inputSchema);

    /**
    * @brief get the result schema of this operator
    * @return Schema
    */
    Schema getOutputSchema() const override;

    /**
     * @brief set the result schema of this operator
     * @param outputSchema
    */
    void setOutputSchema(Schema outputSchema) override;

    /**
     * @brief Set the input origin ids from the input stream
     * @param originIds
     */
    void setInputOriginIds(const std::vector<OriginId>& originIds);

    /**
     * @brief Gets the input origin ids  from the input stream
     * @return std::vector<OriginId>
     */
    std::vector<OriginId> getInputOriginIds() const;

    /**
     * @brief Gets the output origin ids from this operator
     * @return std::vector<OriginId>
     */
    std::vector<OriginId> getOutputOriginIds() const override;

protected:
    std::string toString() const override;

    Schema inputSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
    Schema outputSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
    std::vector<OriginId> inputOriginIds;
};

}
