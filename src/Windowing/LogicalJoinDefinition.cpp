#include <Util/Logger.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
namespace NES::Join {

LogicalJoinDefinition::LogicalJoinDefinition(FieldAccessExpressionNodePtr joinKey,
                                             Windowing::WindowTypePtr windowType,
                                             DistributionCharacteristicPtr distributionType,
                                             WindowTriggerPolicyPtr triggerPolicy,
                                             BaseJoinActionDescriptorPtr triggerAction)
    : joinKey(joinKey),
      windowType(windowType),
      distributionType(distributionType),
      triggerPolicy(triggerPolicy),
      triggerAction(triggerAction) {
}
LogicalJoinDefinitionPtr LogicalJoinDefinition::create(FieldAccessExpressionNodePtr joinKey,
                                                       Windowing::WindowTypePtr windowType,
                                                       DistributionCharacteristicPtr distributionType,
                                                       WindowTriggerPolicyPtr triggerPolicy,
                                                       BaseJoinActionDescriptorPtr triggerAction) {
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

Join::BaseJoinActionDescriptorPtr LogicalJoinDefinition::getTriggerAction() const {
    return triggerAction;
}

Windowing::DistributionCharacteristicPtr LogicalJoinDefinition::getDistributionType() const {
    return distributionType;
}

size_t LogicalJoinDefinition::getNumberOfInputEdges()
{
    //TODO: replace this with a different number, an issue for this exists
    return 2;
}


};// namespace NES::Join
