#include <API/AttributeField.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Window/DistributionCharacteristic.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>

namespace NES {

DistributionCharacteristic::DistributionCharacteristic(Type type) : type(type) {}

DistributionCharacteristicPtr DistributionCharacteristic::createCentralizedWindowType() {
    return std::make_shared<DistributionCharacteristic>(Type::Centralized);
}

DistributionCharacteristicPtr DistributionCharacteristic::createDistributedWindowType() {
    return std::make_shared<DistributionCharacteristic>(Type::Distributed);
}

DistributionCharacteristic::Type DistributionCharacteristic::getType() {
    return type;
}

}// namespace NES