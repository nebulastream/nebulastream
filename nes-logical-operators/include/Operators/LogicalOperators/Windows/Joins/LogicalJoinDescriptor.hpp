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

#include <cstdint>
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Types/WindowType.hpp>

namespace NES::Join
{
/**
 * @brief Runtime definition of a join operator
 * @experimental
 */
class LogicalJoinDescriptor
{
public:
    /**
     * With this enum we distinguish between options to compose two sources, in particular, we reuse Join Logic for binary CEP operators which require a Cartesian product.
     * Thus, INNER_JOIN combines two tuples in case they share a common key attribute
     * CARTESIAN_PRODUCT combines two tuples regardless if they share a common attribute.
     *
     * Example:
     * Source1: {(key1,2),(key2,3)}
     * Source2: {(key1,2),(key2,3)}
     *
     * INNER_JOIN: {(Key1,2,2), (key2,3,3)}
     * CARTESIAN_PRODUCT: {(key1,2,key1,2),(key1,2,key2,3), (key2,3,key1,2), (key2,3,key2,3)}
     *
     */
    enum class JoinType : uint8_t
    {
        INNER_JOIN,
        CARTESIAN_PRODUCT
    };

    static std::shared_ptr<LogicalJoinDescriptor> create(
        NodeFunctionPtr joinFunction,
        const Windowing::WindowTypePtr& windowType,
        uint64_t numberOfInputEdgesLeft,
        uint64_t numberOfInputEdgesRight,
        JoinType joinType);

    explicit LogicalJoinDescriptor(
        NodeFunctionPtr joinFunction,
        Windowing::WindowTypePtr windowType,
        uint64_t numberOfInputEdgesLeft,
        uint64_t numberOfInputEdgesRight,
        JoinType joinType,
        OriginId originId = INVALID_ORIGIN_ID);

    /**
   * @brief getter left source type
   */
    SchemaPtr getLeftSourceType() const;

    /**
   * @brief getter of right source type
   */
    SchemaPtr getRightSourceType() const;

    /**
     * @brief getter/setter for window type
    */
    Windowing::WindowTypePtr getWindowType() const;

    /**
     * @brief getter for on trigger action
     * @return trigger action
    */
    [[nodiscard]] JoinType getJoinType() const;

    /**
     * @brief number of input edges. Need to define a clear concept for this
     * @experimental This is experimental API
     * @return
     */
    uint64_t getNumberOfInputEdgesLeft() const;

    /**
     * @brief number of input edges. Need to define a clear concept for this
     * @return
     */
    uint64_t getNumberOfInputEdgesRight() const;

    /**
     * @brief Update the left and right source types upon type inference
     * @param leftSourceType the type of the left source
     * @param rightSourceType the type of the right source
     */
    void updateSourceTypes(SchemaPtr leftSourceType, SchemaPtr rightSourceType);

    /**
     * @brief Update the output source type upon type inference
     * @param outputSchema the type of the output source
     */
    void updateOutputDefinition(SchemaPtr outputSchema);

    /**
     * @brief Getter of the output source schema
     * @return the output source schema
     */
    [[nodiscard]] SchemaPtr getOutputSchema() const;

    void setNumberOfInputEdgesLeft(uint64_t numberOfInputEdgesLeft);
    void setNumberOfInputEdgesRight(uint64_t numberOfInputEdgesRight);

    /**
     * @brief Getter for the origin id of this window.
     * @return origin id
     */
    [[nodiscard]] OriginId getOriginId() const;

    /**
     * @brief Setter for the origin id
     * @param originId
     */
    void setOriginId(OriginId originId);

    /**
     * @brief Getter keys
     * @return keys
     */
    [[nodiscard]] NodeFunctionPtr getJoinFunction();

    /**
     * @brief Checks if these two are equal
     * @param other: LogicalJoinDescriptor that we want to check if they are equal
     * @return Boolean
     */
    bool equals(const LogicalJoinDescriptor& other) const;

private:
    NodeFunctionPtr joinFunction;
    SchemaPtr leftSourceType = Schema::create();
    SchemaPtr rightSourceType = Schema::create();
    SchemaPtr outputSchema = Schema::create();
    Windowing::WindowTypePtr windowType;
    uint64_t numberOfInputEdgesLeft;
    uint64_t numberOfInputEdgesRight;
    JoinType joinType;
    OriginId originId;
};

using LogicalJoinDescriptorPtr = std::shared_ptr<LogicalJoinDescriptor>;
}
