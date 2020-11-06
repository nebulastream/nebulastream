#include <Windowing/LogicalJoinDefinition.hpp>
namespace NES::Join {

LogicalJoinDefinition::LogicalJoinDefinition(FieldAccessExpressionNodePtr joinKey,
                                             FieldAccessExpressionNodePtr leftJoinValue, FieldAccessExpressionNodePtr rightJoinValue,
                                             Windowing::WindowTypePtr windowType,
                                             WindowTriggerPolicyPtr triggerPolicy)
    : joinKey(joinKey),
      leftJoinValue(leftJoinValue),
      rightJoinValue(rightJoinValue),
      windowType(windowType),
      triggerPolicy(triggerPolicy) {
}

LogicalJoinDefinitionPtr LogicalJoinDefinition::create(FieldAccessExpressionNodePtr joinKey,
                                                       FieldAccessExpressionNodePtr leftJoinValue, FieldAccessExpressionNodePtr rightJoinValue,
                                                       Windowing::WindowTypePtr windowType,
                                                       WindowTriggerPolicyPtr triggerPolicy) {
    return std::make_shared<Join::LogicalJoinDefinition>(joinKey, leftJoinValue, rightJoinValue, windowType, triggerPolicy);
}

FieldAccessExpressionNodePtr LogicalJoinDefinition::getJoinKey() {
    return joinKey;
}

void LogicalJoinDefinition::setJoinKey(FieldAccessExpressionNodePtr key) {
    this->joinKey = std::move(key);
}

FieldAccessExpressionNodePtr LogicalJoinDefinition::getLeftJoinValue() {
    return leftJoinValue;
}

void LogicalJoinDefinition::setLeftJoinValue(FieldAccessExpressionNodePtr key) {
    this->leftJoinValue = std::move(key);
}

FieldAccessExpressionNodePtr LogicalJoinDefinition::getRightJoinValue() {
    return rightJoinValue;
}

void LogicalJoinDefinition::setRightJoinValue(FieldAccessExpressionNodePtr key) {
    this->rightJoinValue = std::move(key);
}

Windowing::WindowTypePtr LogicalJoinDefinition::getWindowType() {
    return windowType;
}

void LogicalJoinDefinition::setWindowType(Windowing::WindowTypePtr windowType) {
    this->windowType = std::move(windowType);
}

Windowing::WindowTriggerPolicyPtr LogicalJoinDefinition::getTriggerPolicy() const {
    return triggerPolicy;
}
void LogicalJoinDefinition::setTriggerPolicy(Windowing::WindowTriggerPolicyPtr triggerPolicy) {
    this->triggerPolicy = triggerPolicy;
}

Windowing::WindowActionDescriptorPtr LogicalJoinDefinition::getTriggerAction() const {
    return triggerAction;
}
};// namespace NES::Join
