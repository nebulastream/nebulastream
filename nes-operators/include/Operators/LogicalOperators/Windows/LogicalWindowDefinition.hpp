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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_LOGICALWINDOWDEFINITION_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_LOGICALWINDOWDEFINITION_HPP_

#include <Identifiers.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <vector>

namespace NES::Windowing {

/**
 * @brief The logical window definition encapsulates all information, which are required for windowed aggregations on data streams.
 * It contains the key attributes, the aggregation functions, and the window type.
 */
class LogicalWindowDefinition {
  public:
    /**
     * @brief This constructor constructs a logical window definition
     * @param keys keys on which the window is constructed
     * @param windowAggregations aggregationFunctions
     * @param windowType type of the window
     * @param distChar
     * @param numberOfInputEdges
     * @param window trigger policy
     * @param window action
     * @param allowedLateness
     */
    explicit LogicalWindowDefinition(std::vector<FieldAccessExpressionNodePtr> keys,
                                     std::vector<WindowAggregationDescriptorPtr> windowAggregations,
                                     WindowTypePtr windowType,
                                     uint64_t allowedLateness);

    /**
     * @brief Create a new window definition for a global window
     * @param windowAggregations
     * @param windowType
     * @param window trigger policy
     * @param numberOfInputEdges
     * @param window action
     * @param allowedLateness
     * @return Window Definition
     */
    static LogicalWindowDefinitionPtr create(std::vector<WindowAggregationDescriptorPtr> windowAggregations,
                                             const WindowTypePtr& windowType,
                                             uint64_t allowedLateness);

    /**
     * @brief Create a new window definition for a keyed window
     * @param keys
     * @param windowAggregation
     * @param windowType
     * @param window trigger policy
     * @param window action
     * @param allowedLateness
     * @return Window Definition
     */
    static LogicalWindowDefinitionPtr create(std::vector<FieldAccessExpressionNodePtr> keys,
                                             std::vector<WindowAggregationDescriptorPtr> windowAggregation,
                                             const WindowTypePtr& windowType,
                                             uint64_t allowedLateness);

    /**
     * @brief Returns true if this window is keyed.
     * @return true if keyed.
    */
    bool isKeyed();

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
    std::vector<WindowAggregationDescriptorPtr> getWindowAggregation();

    /**
     * @brief Sets the list of window aggregations.
     * @param windowAggregation
     */
    void setWindowAggregation(std::vector<WindowAggregationDescriptorPtr> windowAggregation);

    /**
     * @brief Getter for the window type.
     */
    WindowTypePtr getWindowType();

    /**
     * @brief Setter of the window type.
     * @param windowType
     */
    void setWindowType(WindowTypePtr windowType);

    /**
     * @brief Getter for the key attributes.
     * @return Vector of key attributes.
     */
    std::vector<FieldAccessExpressionNodePtr> getKeys();

    /**
     * @brief Setter for the keys.
     * @param keys
     */
    void setOnKey(std::vector<FieldAccessExpressionNodePtr> keys);

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
    [[nodiscard]] uint64_t getOriginId() const;

    /**
     * @brief Setter for the origin id
     * @param originId
     */
    void setOriginId(OriginId originId);

    /**
     * @brief Creates a copy of the logical window definition
     * @return LogicalWindowDefinitionPtr
     */
    LogicalWindowDefinitionPtr copy();

    /**
    * @brief Getter for on trigger action
     * @return trigger action
    */
    [[nodiscard]] WindowActionDescriptorPtr getTriggerAction() const;

    /**
     * @brief To string function for the window definition.
     * @return string
     */
    std::string toString();

    /**
     * @brief Checks if the input window definition is equal to this window definition by comparing the window key, type,
     * and aggregation
     * @param otherWindowDefinition: The other window definition
     * @return true if they are equal else false
     */
    bool equal(LogicalWindowDefinitionPtr otherWindowDefinition);
    const std::vector<OriginId>& getInputOriginIds() const;
    void setInputOriginIds(const std::vector<OriginId>& inputOriginIds);

  private:
    std::vector<WindowAggregationDescriptorPtr> windowAggregation;
    WindowTypePtr windowType;
    std::vector<FieldAccessExpressionNodePtr> onKey;
    uint64_t numberOfInputEdges = 0;
    std::vector<OriginId> inputOriginIds;
    OriginId originId{};
    uint64_t allowedLateness;
};

}// namespace NES::Windowing

#endif // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_LOGICALWINDOWDEFINITION_HPP_
