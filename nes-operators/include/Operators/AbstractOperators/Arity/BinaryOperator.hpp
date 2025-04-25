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
 * @brief A binary operator with more the none input operator, thus it has a left and a right input schema.
 */
class BinaryOperator : public virtual Operator
{
public:
    explicit BinaryOperator(OperatorId id);

    /**
   * @brief get the input schema of this operator from the left side
   * @return Schema
   */
    Schema getLeftInputSchema() const;

    /**
    * @brief set the input schema of this operator for the left side
     * @param inputSchema
    */
    void setLeftInputSchema(const Schema& inputSchema);

    /**
    * @brief get the input schema of this operator from the left side
    * @return Schema
    */
    Schema getRightInputSchema() const;

    /**
     * @brief set the input schema of this operator for the right side
     * @param inputSchema
    */
    void setRightInputSchema(const Schema& inputSchema);

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
     * @brief Set the input origin ids for the left input stream.
     * @param originIds
     */
    void setLeftInputOriginIds(const std::vector<OriginId>& originIds);

    /**
     * @brief Gets the input origin ids for the left input stream
     * @return std::vector<OriginId>
     */
    virtual std::vector<OriginId> getLeftInputOriginIds();

    /**
     * @brief Gets the input origin from both sides
     * @return std::vector<OriginId>
     */
    virtual std::vector<OriginId> getAllInputOriginIds();

    /**
     * @brief Set the input origin ids for the right input stream.
     * @param originIds
     */
    void setRightInputOriginIds(const std::vector<OriginId>& originIds);

    /**
     * @brief Gets the input origin ids for the right input stream
     * @return std::vector<OriginId>
     */
    virtual std::vector<OriginId> getRightInputOriginIds();

    /**
     * @brief Gets the output origin ids for the result stream
     * @return std::vector<OriginId> originids
     */
    std::vector<OriginId> getOutputOriginIds() const override;

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;

    Schema leftInputSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
    Schema rightInputSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
    Schema outputSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
    std::vector<Schema> distinctSchemas;
    std::vector<OriginId> leftInputOriginIds;
    std::vector<OriginId> rightInputOriginIds;
};

}
