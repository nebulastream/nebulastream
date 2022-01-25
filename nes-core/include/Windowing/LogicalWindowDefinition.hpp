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

#ifndef NES_INCLUDE_WINDOWING_LOGICALWINDOWDEFINITION_HPP_
#define NES_INCLUDE_WINDOWING_LOGICALWINDOWDEFINITION_HPP_

#include <Windowing/WindowingForwardRefs.hpp>

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
                                     std::vector<WindowAggregationPtr> windowAggregations,
                                     WindowTypePtr windowType,
                                     DistributionCharacteristicPtr distChar,
                                     uint64_t numberOfInputEdges,
                                     WindowTriggerPolicyPtr triggerPolicy,
                                     WindowActionDescriptorPtr triggerAction,
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
    static LogicalWindowDefinitionPtr create(std::vector<WindowAggregationPtr> windowAggregations,
                                             const WindowTypePtr& windowType,
                                             const DistributionCharacteristicPtr& distChar,
                                             uint64_t numberOfInputEdges,
                                             const WindowTriggerPolicyPtr& triggerPolicy,
                                             const WindowActionDescriptorPtr& triggerAction,
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
                                             std::vector<WindowAggregationPtr> windowAggregation,
                                             const WindowTypePtr& windowType,
                                             const DistributionCharacteristicPtr& distChar,
                                             uint64_t numberOfInputEdges,
                                             const WindowTriggerPolicyPtr& triggerPolicy,
                                             const WindowActionDescriptorPtr& triggerAction,
                                             uint64_t allowedLateness);

    /**
     * @brief Returns true if this window is keyed.
     * @return true if keyed.
    */
    bool isKeyed();

    /**
     * @brief Setter for the distribution type (centralized or distributed).
     * @deprecated Will be removed to an seperated operator in the future.
     */
    void setDistributionCharacteristic(DistributionCharacteristicPtr characteristic);

    /**
     * @brief Getter for the distribution type.
     * @deprecated Will be removed to an seperated operator in the future.
     * @return DistributionCharacteristicPtr
     */
    DistributionCharacteristicPtr getDistributionType();

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
    std::vector<WindowAggregationPtr> getWindowAggregation();

    /**
     * @brief Sets the list of window aggregations.
     * @param windowAggregation
     */
    void setWindowAggregation(std::vector<WindowAggregationPtr> windowAggregation);

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
    void setOriginId(uint64_t originId);

    /**
     * @brief Creates a copy of the logical window definition
     * @return LogicalWindowDefinitionPtr
     */
    LogicalWindowDefinitionPtr copy();

    /**
     * @brief Getter for on trigger policy.
     * @return WindowTriggerPolicyPtr
     */
    [[nodiscard]] WindowTriggerPolicyPtr getTriggerPolicy() const;

    /**
     * @brief Setter for the trigger policy.
     * @param triggerPolicy
     */
    void setTriggerPolicy(WindowTriggerPolicyPtr triggerPolicy);

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

  private:
    std::vector<WindowAggregationPtr> windowAggregation;
    WindowTriggerPolicyPtr triggerPolicy;
    WindowActionDescriptorPtr triggerAction;
    WindowTypePtr windowType;
    std::vector<FieldAccessExpressionNodePtr> onKey;
    DistributionCharacteristicPtr distributionType;
    uint64_t numberOfInputEdges;
    uint64_t originId{};
    uint64_t allowedLateness;
};

}// namespace NES::Windowing

#endif  // NES_INCLUDE_WINDOWING_LOGICALWINDOWDEFINITION_HPP_
