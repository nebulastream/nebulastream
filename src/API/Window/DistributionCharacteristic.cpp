#include <API/AttributeField.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Window/DistributionCharacteristic.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>

namespace NES {

DistributionCharacteristic::DistributionCharacteristic(Type type) : type(type) {}

DistributionCharacteristicPtr DistributionCharacteristic::createCompleteWindowType() {
    return std::make_shared<DistributionCharacteristic>(Type::Complete);
}

DistributionCharacteristicPtr DistributionCharacteristic::createSlicingWindowType() {
    return std::make_shared<DistributionCharacteristic>(Type::Slicing);
}

DistributionCharacteristicPtr DistributionCharacteristic::createCombiningWindowType() {
    return std::make_shared<DistributionCharacteristic>(Type::Combining);
}

DistributionCharacteristic::Type DistributionCharacteristic::getType() {
    return type;
}

}// namespace nes