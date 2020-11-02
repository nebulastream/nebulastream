#include <Join/LogicalJoinDefinition.hpp>
namespace NES::Join {

LogicalJoinDefinition::LogicalJoinDefinition(FieldAccessExpressionNodePtr leftJoinKey, FieldAccessExpressionNodePtr rightJoinKey, Windowing::WindowTypePtr windowType)
    : leftJoinKey(leftJoinKey),
      rightJoinKey(rightJoinKey),
      windowType(windowType) {
}

LogicalJoinDefinitionPtr LogicalJoinDefinition::create(FieldAccessExpressionNodePtr leftJoinKey, FieldAccessExpressionNodePtr rightJoinKey, Windowing::WindowTypePtr windowType) {
    return std::make_shared<Join::LogicalJoinDefinition>(leftJoinKey, rightJoinKey, windowType);
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

Windowing::WindowTypePtr LogicalJoinDefinition::getWindowType() {
    return windowType;
}

void LogicalJoinDefinition::setWindowType(Windowing::WindowTypePtr windowType) {
    this->windowType = std::move(windowType);
}

};// namespace NES::Join
