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
#include <Windowing/WindowHandler/JoinForwardRefs.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Join {
using namespace Windowing;

class LogicalJoinDefinition {
  public:
    static LogicalJoinDefinitionPtr create(FieldAccessExpressionNodePtr joinKey, FieldAccessExpressionNodePtr leftJoinValue, FieldAccessExpressionNodePtr rightJoinValue, Windowing::WindowTypePtr windowType, WindowTriggerPolicyPtr triggerPolicy);

    static LogicalJoinDefinitionPtr create(FieldAccessExpressionNodePtr joinKey, Windowing::WindowTypePtr windowType, WindowTriggerPolicyPtr triggerPolicy);

    LogicalJoinDefinition(FieldAccessExpressionNodePtr joinKey, FieldAccessExpressionNodePtr leftJoinValue, FieldAccessExpressionNodePtr rightJoinValue, Windowing::WindowTypePtr windowType, WindowTriggerPolicyPtr triggerPolicy);

    LogicalJoinDefinition(FieldAccessExpressionNodePtr joinKey, Windowing::WindowTypePtr windowType, WindowTriggerPolicyPtr triggerPolicy);

    /**
    * @brief getter/setter for on left join key
    */
    FieldAccessExpressionNodePtr getJoinKey();
    void setJoinKey(FieldAccessExpressionNodePtr key);

    /**
    * @brief getter/setter for on left join value
    */
    FieldAccessExpressionNodePtr getLeftJoinValue();
    void setLeftJoinValue(FieldAccessExpressionNodePtr key);

    /**
     * @brief getter/setter for on right join key
     */
    FieldAccessExpressionNodePtr getRightJoinValue();
    void setRightJoinValue(FieldAccessExpressionNodePtr key);

    /**
     * @brief getter/setter for window type
    */
    Windowing::WindowTypePtr getWindowType();
    void setWindowType(Windowing::WindowTypePtr windowType);

    /**
     * @brief getter/setter for on trigger policy
     */
    WindowTriggerPolicyPtr getTriggerPolicy() const;
    void setTriggerPolicy(WindowTriggerPolicyPtr triggerPolicy);

    /**
    * @brief getter for on trigger action
     * @return trigger action
    */
    WindowActionDescriptorPtr getTriggerAction() const;

  private:
    FieldAccessExpressionNodePtr joinKey;
    FieldAccessExpressionNodePtr leftJoinValue;
    FieldAccessExpressionNodePtr rightJoinValue;
    WindowTriggerPolicyPtr triggerPolicy;
    WindowActionDescriptorPtr triggerAction;
    Windowing::WindowTypePtr windowType;
};

typedef std::shared_ptr<LogicalJoinDefinition> LogicalJoinDefinitionPtr;
}// namespace NES::Join
#endif//NES_INCLUDE_JOIN_LOGICALJOINDEFINITION_HPP_
