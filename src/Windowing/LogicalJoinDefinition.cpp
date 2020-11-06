#include <Windowing/LogicalJoinDefinition.hpp>
namespace NES::Join {

LogicalJoinDefinition::LogicalJoinDefinition(FieldAccessExpressionNodePtr leftJoinKey, FieldAccessExpressionNodePtr rightJoinKey,
                                             FieldAccessExpressionNodePtr leftJoinValue, FieldAccessExpressionNodePtr rightJoinValue,
                                             Windowing::WindowTypePtr windowType)
    : leftJoinKey(leftJoinKey),
      rightJoinKey(rightJoinKey),
      leftJoinValue(leftJoinValue),
      rightJoinValue(rightJoinValue),
      windowType(windowType) {
}

LogicalJoinDefinitionPtr LogicalJoinDefinition::create(FieldAccessExpressionNodePtr leftJoinKey, FieldAccessExpressionNodePtr rightJoinKey,
                                                       FieldAccessExpressionNodePtr leftJoinValue, FieldAccessExpressionNodePtr rightJoinValue,
                                                       Windowing::WindowTypePtr windowType) {
    return std::make_shared<Join::LogicalJoinDefinition>(leftJoinKey, rightJoinKey, leftJoinValue, rightJoinValue, windowType);
}

FieldAccessExpressionNodePtr LogicalJoinDefinition::getLeftJoinKey() {
    return leftJoinKey;
}

void LogicalJoinDefinition::setLeftJoinKey(FieldAccessExpressionNodePtr key) {
    this->leftJoinKey = std::move(key);
}

FieldAccessExpressionNodePtr LogicalJoinDefinition::getRightJoinKey() {
    return rightJoinKey;
}

void LogicalJoinDefinition::setRightJoinKey(FieldAccessExpressionNodePtr key) {
    this->rightJoinKey = std::move(key);
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

};// namespace NES::Join
