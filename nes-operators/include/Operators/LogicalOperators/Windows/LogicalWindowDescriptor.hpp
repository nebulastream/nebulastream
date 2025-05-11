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
#include <vector>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Types/WindowType.hpp>

namespace NES::Windowing
{
/**
 * @brief The logical window definition encapsulates all information, which are required for windowed aggregations on data streams.
 * It contains the key attributes, the aggregation functions, and the window type.
 */
class LogicalWindowDescriptor
{
public:
    explicit LogicalWindowDescriptor(
        const std::vector<std::shared_ptr<NodeFunctionFieldAccess>>& keys,
        std::vector<std::shared_ptr<WindowAggregationDescriptor>> windowAggregation,
        std::shared_ptr<WindowType> windowType);

    static std::shared_ptr<LogicalWindowDescriptor> create(
        const std::vector<std::shared_ptr<WindowAggregationDescriptor>>& windowAggregations, const std::shared_ptr<WindowType>& windowType);

    static std::shared_ptr<LogicalWindowDescriptor> create(
        const std::vector<std::shared_ptr<NodeFunctionFieldAccess>>& keys,
        const std::vector<std::shared_ptr<WindowAggregationDescriptor>>& windowAggregation,
        const std::shared_ptr<WindowType>& windowType);

    /**
     * @brief Returns true if this window is keyed.
     * @return true if keyed.
    */
    bool isKeyed() const;

    /**
     * @brief Getter for the number of input edges, which is used for the low watermarks.
     */
    [[nodiscard]] uint64_t getNumberOfInputEdges() const;

    /**
     * @brief Setter for the number of input edges.
     * @param numberOfInputEdges
     */
    void setNumberOfInputEdges(uint64_t numberOfInputEdges);

    /**
     * @brief Getter for the aggregation functions.
     * @return Vector of WindowAggregations.
     */
    [[nodiscard]] std::vector<std::shared_ptr<WindowAggregationDescriptor>> getWindowAggregation() const;

    /**
     * @brief Sets the list of window aggregations.
     * @param windowAggregation
     */
    void setWindowAggregation(const std::vector<std::shared_ptr<WindowAggregationDescriptor>>& windowAggregation);

    /**
     * @brief Getter for the window type.
     */
    [[nodiscard]] std::shared_ptr<WindowType> getWindowType() const;

    /**
     * @brief Setter of the window type.
     * @param windowType
     */
    void setWindowType(std::shared_ptr<WindowType> windowType);

    [[nodiscard]] std::vector<std::shared_ptr<NodeFunctionFieldAccess>> getKeys() const;
    void setOnKey(const std::vector<std::shared_ptr<NodeFunctionFieldAccess>>& onKeys);

    /**
     * @brief Getter for the allowed lateness. The allowed lateness defines,
     * how long the system should wait for out of order events before a window is triggered.
     * @return time in milliseconds.
     */
    [[nodiscard]] uint64_t getAllowedLateness() const;

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
     * @brief Creates a copy of the logical window definition
     * @return std::shared_ptr<LogicalWindowDescriptor>
     */
    std::shared_ptr<LogicalWindowDescriptor> copy() const;

    /**
     * @brief To string function for the window definition.
     * @return string
     */
    std::string toString() const;

    /**
     * @brief Checks if the input window definition is equal to this window definition by comparing the window key, type,
     * and aggregation
     * @param otherWindowDefinition: The other window definition
     * @return true if they are equal else false
     */
    [[nodiscard]] bool equal(const std::shared_ptr<LogicalWindowDescriptor>& otherWindowDefinition) const;
    [[nodiscard]] const std::vector<OriginId>& getInputOriginIds() const;
    void setInputOriginIds(const std::vector<OriginId>& inputOriginIds);

    /**
     * @return true if any of the AggregationFunctions require sequential processing
     */
    [[nodiscard]] bool requiresSequentialAggregation() const;

private:
    std::vector<std::shared_ptr<WindowAggregationDescriptor>> windowAggregation;
    std::shared_ptr<WindowType> windowType;
    std::vector<std::shared_ptr<NodeFunctionFieldAccess>> onKey;
    uint64_t numberOfInputEdges = 0;
    std::vector<OriginId> inputOriginIds;
    OriginId originId = INVALID_ORIGIN_ID;
};
}
