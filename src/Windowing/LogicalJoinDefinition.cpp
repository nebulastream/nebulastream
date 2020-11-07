#include <Util/Logger.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
namespace NES::Join {

LogicalJoinDefinition::LogicalJoinDefinition(FieldAccessExpressionNodePtr joinKey,
                                             Windowing::WindowTypePtr windowType,
                                             DistributionCharacteristicPtr distributionType,
                                             WindowTriggerPolicyPtr triggerPolicy,
                                             WindowActionDescriptorPtr triggerAction)
    : joinKey(joinKey),
      windowType(windowType),
      distributionType(distributionType),
      triggerPolicy(triggerPolicy),
      triggerAction(triggerAction){
}
LogicalJoinDefinitionPtr LogicalJoinDefinition::create(FieldAccessExpressionNodePtr joinKey,
                                                       Windowing::WindowTypePtr windowType,
                                                       DistributionCharacteristicPtr distributionType,
                                                       WindowTriggerPolicyPtr triggerPolicy,
                                                       WindowActionDescriptorPtr triggerAction) {
    return std::make_shared<Join::LogicalJoinDefinition>(joinKey, windowType, distributionType, triggerPolicy, triggerAction);
}

FieldAccessExpressionNodePtr LogicalJoinDefinition::getJoinKey() {
    return joinKey;
}

Windowing::WindowTypePtr LogicalJoinDefinition::getWindowType() {
    return windowType;
}

Windowing::WindowTriggerPolicyPtr LogicalJoinDefinition::getTriggerPolicy() const {
    return triggerPolicy;
}

Windowing::WindowActionDescriptorPtr LogicalJoinDefinition::getTriggerAction() const {
    return triggerAction;
}

Windowing::DistributionCharacteristicPtr LogicalJoinDefinition::getDistributionType() const {
    return distributionType;
}

};// namespace NES::Join
