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

#include <Util/Logger.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
namespace NES::Join {

LogicalJoinDefinition::LogicalJoinDefinition(FieldAccessExpressionNodePtr joinKey, Windowing::WindowTypePtr windowType,
                                             Windowing::DistributionCharacteristicPtr distributionType,
                                             Windowing::WindowTriggerPolicyPtr triggerPolicy,
                                             BaseJoinActionDescriptorPtr triggerAction)
    : joinKey(joinKey), windowType(windowType), distributionType(distributionType), triggerPolicy(triggerPolicy),
      triggerAction(triggerAction) {}
LogicalJoinDefinitionPtr LogicalJoinDefinition::create(FieldAccessExpressionNodePtr joinKey, Windowing::WindowTypePtr windowType,
                                                       Windowing::DistributionCharacteristicPtr distributionType,
                                                       Windowing::WindowTriggerPolicyPtr triggerPolicy,
                                                       BaseJoinActionDescriptorPtr triggerAction) {
    return std::make_shared<Join::LogicalJoinDefinition>(joinKey, windowType, distributionType, triggerPolicy, triggerAction);
}

FieldAccessExpressionNodePtr LogicalJoinDefinition::getJoinKey() { return joinKey; }

Windowing::WindowTypePtr LogicalJoinDefinition::getWindowType() { return windowType; }

Windowing::WindowTriggerPolicyPtr LogicalJoinDefinition::getTriggerPolicy() const { return triggerPolicy; }

Join::BaseJoinActionDescriptorPtr LogicalJoinDefinition::getTriggerAction() const { return triggerAction; }

Windowing::DistributionCharacteristicPtr LogicalJoinDefinition::getDistributionType() const { return distributionType; }

size_t LogicalJoinDefinition::getNumberOfInputEdges() {
    //TODO: replace this with a different number, an issue for this exists
    return 2;
}

};// namespace NES::Join
