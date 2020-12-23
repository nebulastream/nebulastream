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

#ifndef NES_INCLUDE_JOIN_LOGICALJOINDEFINITION_HPP_
#define NES_INCLUDE_JOIN_LOGICALJOINDEFINITION_HPP_
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <cstdint>

namespace NES::Join {

/**
 * @brief Runtime definition of a join operator
 * @experimental
 */
class LogicalJoinDefinition {
  public:
    static LogicalJoinDefinitionPtr create(FieldAccessExpressionNodePtr leftJoinKeyType,
                                           FieldAccessExpressionNodePtr rightJoinKeyType, Windowing::WindowTypePtr windowType,
                                           Windowing::DistributionCharacteristicPtr distributionType,
                                           Windowing::WindowTriggerPolicyPtr triggerPolicy,
                                           BaseJoinActionDescriptorPtr triggerAction, uint64_t numberOfInputEdgesLeft,
                                           uint64_t numberOfInputEdgesRight);

    explicit LogicalJoinDefinition(FieldAccessExpressionNodePtr leftJoinKeyType, FieldAccessExpressionNodePtr rightJoinKeyType,
                                   Windowing::WindowTypePtr windowType, Windowing::DistributionCharacteristicPtr distributionType,
                                   Windowing::WindowTriggerPolicyPtr triggerPolicy, BaseJoinActionDescriptorPtr triggerAction,
                                   uint64_t numberOfInputEdgesLeft, uint64_t numberOfInputEdgesRight);

    /**
    * @brief getter/setter for on left join key
    */
    FieldAccessExpressionNodePtr getLeftJoinKey();

    /**
   * @brief getter/setter for on left join key
   */
    FieldAccessExpressionNodePtr getRightJoinKey();

    /**
   * @brief getter left stream type
   */
    SchemaPtr getLeftStreamType();

    /**
   * @brief getter of right stream type
   */
    SchemaPtr getRightStreamType();

    /**
     * @brief getter/setter for window type
    */
    Windowing::WindowTypePtr getWindowType();

    /**
     * @brief getter for on trigger policy
     */
    Windowing::WindowTriggerPolicyPtr getTriggerPolicy() const;

    /**
     * @brief getter for on trigger action
     * @return trigger action
    */
    BaseJoinActionDescriptorPtr getTriggerAction() const;

    /**
     * @brief getter for on distribution type
     * @return distributionType
    */
    Windowing::DistributionCharacteristicPtr getDistributionType() const;

    /**
     * @brief number of input edges. Need to define a clear concept for this
     * @experimental This is experimental API
     * @return
     */
    uint64_t getNumberOfInputEdgesLeft();

    /**
     * @brief number of input edges. Need to define a clear concept for this
     * @return
     */
    uint64_t getNumberOfInputEdgesRight();

    /**
     * @brief Update the left and right stream types upon type inference
     * @param leftStreamType the type of the left stream
     * @param rightStreamType the type of the right stream
     */
    void updateStreamTypes(SchemaPtr leftStreamType, SchemaPtr rightStreamType);

    /**
     * @brief Update the output stream type upon type inference
     * @param outputSchema the type of the output stream
     */
    void updateOutputDefinition(SchemaPtr outputSchema);

    /**
     * @brief Getter of the output stream schema
     * @return the output stream schema
     */
    SchemaPtr getOutputSchema() const;

  private:
    FieldAccessExpressionNodePtr leftJoinKeyType;
    FieldAccessExpressionNodePtr rightJoinKeyType;
    SchemaPtr leftStreamType;
    SchemaPtr rightStreamType;
    SchemaPtr outputSchema;
    Windowing::WindowTriggerPolicyPtr triggerPolicy;
    BaseJoinActionDescriptorPtr triggerAction;
    Windowing::WindowTypePtr windowType;
    Windowing::DistributionCharacteristicPtr distributionType;
    uint64_t numberOfInputEdgesLeft;
    uint64_t numberOfInputEdgesRight;
};

typedef std::shared_ptr<LogicalJoinDefinition> LogicalJoinDefinitionPtr;
}// namespace NES::Join
#endif//NES_INCLUDE_JOIN_LOGICALJOINDEFINITION_HPP_
