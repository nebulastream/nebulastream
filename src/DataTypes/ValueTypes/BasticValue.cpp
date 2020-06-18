
#include <DataTypes/DataType.hpp>
#include <DataTypes/ValueTypes/BasicValue.hpp>
namespace NES {

BasicValue::BasicValue(DataTypePtr type, std::string value) : ValueType(type), value(value) {}

bool BasicValue::isBasicValue() {
    return true;
}

std::string BasicValue::toString() {
    return "BasicValue";
}

std::string BasicValue::getValue() {
    return value;
}

bool BasicValue::isEquals(ValueTypePtr valueType) {
    if (!valueType->isBasicValue())
        return false;
    auto otherBasicValue = std::dynamic_pointer_cast<BasicValue>(valueType);
    return otherBasicValue->getType()->isEquals(getType()) && value == otherBasicValue->value;
}

}// namespace NES