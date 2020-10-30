#include <Join/LogicalJoinDefinition.hpp>
namespace NES::Join {

LogicalJoinDefinition::LogicalJoinDefinition()
{

}

LogicalJoinDefinitionPtr LogicalJoinDefinition::create()
{
    return std::make_shared<Join::LogicalJoinDefinition>();
}

FieldAccessExpressionNodePtr LogicalJoinDefinition::getOnKey() {
    return onKey;
}

void LogicalJoinDefinition::setOnKey(FieldAccessExpressionNodePtr onKey) {
    this->onKey = std::move(onKey);
}


Windowing::WindowTypePtr LogicalJoinDefinition::getWindowType() {
    return windowType;
}

void LogicalJoinDefinition::setWindowType(Windowing::WindowTypePtr windowType) {
    this->windowType = std::move(windowType);
}

};// namespace NES::Join
