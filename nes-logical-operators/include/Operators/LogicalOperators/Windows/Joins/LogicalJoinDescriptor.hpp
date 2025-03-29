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
#include <memory>
#include <API/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace NES::Join
{

class LogicalJoinDescriptor
{
public:
    /// With this enum we distinguish between options to compose two sources, in particular, we reuse Join Logic for binary CEP operators which require a Cartesian product.
    /// Thus, INNER_JOIN combines two tuples in case they share a common key attribute
    /// CARTESIAN_PRODUCT combines two tuples regardless if they share a common attribute.
    ///
    /// Example:
    /// Source1: {(key1,2),(key2,3)}
    /// Source2: {(key1,2),(key2,3)}
    ///
    /// INNER_JOIN: {(Key1,2,2), (key2,3,3)}
    /// CARTESIAN_PRODUCT: {(key1,2,key1,2),(key1,2,key2,3), (key2,3,key1,2), (key2,3,key2,3)}
    enum class JoinType : uint8_t
    {
        INNER_JOIN,
        CARTESIAN_PRODUCT
    };

    static std::shared_ptr<LogicalJoinDescriptor> create(
        std::shared_ptr<LogicalFunction> joinFunction,
        const std::shared_ptr<Windowing::WindowType>& windowType,
        uint64_t numberOfInputEdgesLeft,
        uint64_t numberOfInputEdgesRight,
        JoinType joinType);

    explicit LogicalJoinDescriptor(
        std::shared_ptr<LogicalFunction> joinFunction,
        std::shared_ptr<Windowing::WindowType> windowType,
        uint64_t numberOfInputEdgesLeft,
        uint64_t numberOfInputEdgesRight,
        JoinType joinType,
        OriginId originId = INVALID_ORIGIN_ID);

    std::shared_ptr<Schema> getLeftSourceType() const;
    std::shared_ptr<Schema> getRightSourceType() const;

    std::shared_ptr<Windowing::WindowType> getWindowType() const;
    [[nodiscard]] JoinType getJoinType() const;

    uint64_t getNumberOfInputEdgesLeft() const;
    uint64_t getNumberOfInputEdgesRight() const;

    /// @brief Update the left and right source types upon type inference
    void updateSourceTypes(std::shared_ptr<Schema> leftSourceType, std::shared_ptr<Schema> rightSourceType);

    void setOutputSchema(std::shared_ptr<Schema> outputSchema);
    [[nodiscard]] std::shared_ptr<Schema> getOutputSchema() const;

    void setNumberOfInputEdgesLeft(uint64_t numberOfInputEdgesLeft);
    void setNumberOfInputEdgesRight(uint64_t numberOfInputEdgesRight);

    [[nodiscard]] OriginId getOriginId() const;
    void setOriginId(OriginId originId);

    [[nodiscard]] std::shared_ptr<LogicalFunction> getJoinFunction() const;
    bool equals(const LogicalJoinDescriptor& other) const;

private:
    std::shared_ptr<LogicalFunction> joinFunction;
    std::shared_ptr<Schema> leftSourceType = Schema::create();
    std::shared_ptr<Schema> rightSourceType = Schema::create();
    std::shared_ptr<Schema> outputSchema = Schema::create();
    std::shared_ptr<Windowing::WindowType> windowType;
    uint64_t numberOfInputEdgesLeft;
    uint64_t numberOfInputEdgesRight;
    JoinType joinType;
    OriginId originId;
};

}
