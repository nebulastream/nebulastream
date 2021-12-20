/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_WINDOWING_LOGICAL_WINDOW_DEFINITION_HPP_
#define NES_INCLUDE_WINDOWING_LOGICAL_WINDOW_DEFINITION_HPP_

#include <Windowing/WindowingForwardRefs.hpp>
#include <list>


namespace NES::Windowing {

class LogicalWindowDefinition {
  public:
    /**
     * @brief This constructor construts a key-less window
     * @param windowAggregation
     * @param windowType
     * @param distChar
     * @param numberOfInputEdges
     * @param window trigger policy
     * @param window action
     * @param allowedLateness
     */
    explicit LogicalWindowDefinition(WindowAggregationPtr windowAggregation,
                                     WindowTypePtr windowType,
                                     DistributionCharacteristicPtr distChar,
                                     uint64_t numberOfInputEdges,
                                     WindowTriggerPolicyPtr triggerPolicy,
                                     WindowActionDescriptorPtr triggerAction,
                                     uint64_t allowedLateness);

    /**
     * @brief This constructor constructs a key-by window
     * @param keyList key(s) on which the window is constructed as FieldAccessExpressionNodePtr list
     * @param windowAggregation
     * @param windowType
     * @param distChar
     * @param numberOfInputEdges
     * @param window trigger policy
     * @param window action
     * @param allowedLateness
     */
    explicit LogicalWindowDefinition(std::list<FieldAccessExpressionNodePtr> keyList,
                                     WindowAggregationPtr windowAggregation,
                                     WindowTypePtr windowType,
                                     DistributionCharacteristicPtr distChar,
                                     uint64_t numberOfInputEdges,
                                     WindowTriggerPolicyPtr triggerPolicy,
                                     WindowActionDescriptorPtr triggerAction,
                                     uint64_t allowedLateness);

    /**
     * @brief Create a new window definition for a global window
     * @param windowAggregation
     * @param windowType
     * @param window trigger policy
     * @param numberOfInputEdges
     * @param window action
     * @param allowedLateness
     * @return Window Definition
     */
    static LogicalWindowDefinitionPtr create(const WindowAggregationPtr& windowAggregation,
                                             const WindowTypePtr& windowType,
                                             const DistributionCharacteristicPtr& distChar,
                                             uint64_t numberOfInputEdges,
                                             const WindowTriggerPolicyPtr& triggerPolicy,
                                             const WindowActionDescriptorPtr& triggerAction,
                                             uint64_t allowedLateness);

    /**
     * @brief Create a new window definition for a keyed window
     * @param keyList key(s) on which the window is constructed as FieldAccessExpressionNodePtr list
     * @param windowAggregation
     * @param windowType
     * @param window trigger policy
     * @param window action
     * @param allowedLateness
     * @return Window Definition
     */
    static LogicalWindowDefinitionPtr create(const std::list<FieldAccessExpressionNodePtr> keyList,
                                             const WindowAggregationPtr& windowAggregation,
                                             const WindowTypePtr& windowType,
                                             const DistributionCharacteristicPtr& distChar,
                                             uint64_t numberOfInputEdges,
                                             const WindowTriggerPolicyPtr& triggerPolicy,
                                             const WindowActionDescriptorPtr& triggerAction,
                                             uint64_t allowedLateness);

    /**
    * @brief Create a new window definition for a keyed window
    * @param keyList key(s) on which the window is constructed as ExpressionItem list
    * @param windowAggregation
    * @param windowType
    * @param window trigger policy
    * @param window action
     * @param allowedLateness
    * @return Window Definition
    */
    static LogicalWindowDefinitionPtr create(std::list<ExpressionItem> keyList,
                                             const WindowAggregationPtr& windowAggregation,
                                             const WindowTypePtr& windowType,
                                             const DistributionCharacteristicPtr& distChar,
                                             uint64_t numberOfInputEdges,
                                             const WindowTriggerPolicyPtr& triggerPolicy,
                                             const WindowActionDescriptorPtr& triggerAction,
                                             uint64_t allowedLateness);

    /**
     * @brief getter and setter for the distribution type (centralized or distributed)
     */
    void setDistributionCharacteristic(DistributionCharacteristicPtr characteristic);
    DistributionCharacteristicPtr getDistributionType();

    /**
     * @brief getter and setter for the number of input edges, which is used for the low watermarks
     */
    [[nodiscard]] uint64_t getNumberOfInputEdges() const;
    void setNumberOfInputEdges(uint64_t numberOfInputEdges);

    /**
     * @brief Returns true if this window is keyed.
     * @return
    */
    // TODO need to test
    bool isKeyed();

    /**
     * @brief getter/setter for window aggregation
     */
    WindowAggregationPtr getWindowAggregation();
    void setWindowAggregation(WindowAggregationPtr windowAggregation);

    /**
     * @brief getter/setter for window type
     */
    WindowTypePtr getWindowType();
    void setWindowType(WindowTypePtr windowType);

    /**
     * @brief getter/setter for on key list
     */
    std::list<FieldAccessExpressionNodePtr> getKeyList();
    void setKeyList(std::list<FieldAccessExpressionNodePtr> keyList);

    LogicalWindowDefinitionPtr copy();

    /**
     * @brief getter/setter for on trigger policy
     */
    [[nodiscard]] WindowTriggerPolicyPtr getTriggerPolicy() const;
    void setTriggerPolicy(WindowTriggerPolicyPtr triggerPolicy);

    /**
    * @brief getter for on trigger action
     * @return trigger action
    */
    [[nodiscard]] WindowActionDescriptorPtr getTriggerAction() const;

    std::string toString();

    /**
     * @brief Test is the input window definition is equal to this window definition by comparing the window key, type,
     * and aggregation
     * @param otherWindowDefinition: The other window definition
     * @return true if they are equal else false
     */
    bool equal(LogicalWindowDefinitionPtr otherWindowDefinition);

  private:
    WindowAggregationPtr windowAggregation;
    WindowTriggerPolicyPtr triggerPolicy;
    WindowActionDescriptorPtr triggerAction;
    WindowTypePtr windowType;
    std::list<FieldAccessExpressionNodePtr> keyList;
    DistributionCharacteristicPtr distributionType;
    uint64_t numberOfInputEdges;
    uint64_t originId{};
    uint64_t allowedLateness;

  public:
    [[nodiscard]] uint64_t getAllowedLateness() const;
    [[nodiscard]] uint64_t getOriginId() const;
    void setOriginId(uint64_t originId);
};

}// namespace NES::Windowing

#endif// NES_INCLUDE_WINDOWING_LOGICAL_WINDOW_DEFINITION_HPP_
