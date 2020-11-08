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

namespace NES::Join {
using namespace Windowing;

class LogicalJoinDefinition {
  public:
    static LogicalJoinDefinitionPtr create(FieldAccessExpressionNodePtr joinKey, Windowing::WindowTypePtr windowType, DistributionCharacteristicPtr distributionType, WindowTriggerPolicyPtr triggerPolicy, BaseJoinActionDescriptorPtr triggerAction);

    LogicalJoinDefinition(FieldAccessExpressionNodePtr joinKey, Windowing::WindowTypePtr windowType, DistributionCharacteristicPtr distributionType, WindowTriggerPolicyPtr triggerPolicy, BaseJoinActionDescriptorPtr triggerAction);

    /**
    * @brief getter/setter for on left join key
    */
    FieldAccessExpressionNodePtr getJoinKey();

    /**
     * @brief getter/setter for window type
    */
    Windowing::WindowTypePtr getWindowType();

    /**
     * @brief getter for on trigger policy
     */
    WindowTriggerPolicyPtr getTriggerPolicy() const;

    /**
     * @brief getter for on trigger action
     * @return trigger action
    */
    BaseJoinActionDescriptorPtr getTriggerAction() const;

    /**
     * @brief getter for on distribution type
     * @return distributionType
    */
    DistributionCharacteristicPtr getDistributionType() const;

  private:
    FieldAccessExpressionNodePtr joinKey;
    WindowTriggerPolicyPtr triggerPolicy;
    BaseJoinActionDescriptorPtr triggerAction;
    Windowing::WindowTypePtr windowType;
    DistributionCharacteristicPtr distributionType;
};

typedef std::shared_ptr<LogicalJoinDefinition> LogicalJoinDefinitionPtr;
}// namespace NES::Join
#endif//NES_INCLUDE_JOIN_LOGICALJOINDEFINITION_HPP_
