
#include <Common/DataTypes/DataType.hpp>
#include <Common/ValueTypes/ArrayValue.hpp>

namespace NES {

ArrayValue::ArrayValue(DataTypePtr type, std::vector<std::string> values) : ValueType(type), values(values) {
}

std::vector<std::string> ArrayValue::getValues() {
    return values;
}

bool ArrayValue::isArrayValue() {
    return true;
}

std::string ArrayValue::toString() {
    return "ArrayValue";
}

bool ArrayValue::isEquals(ValueTypePtr valueType) {
    if (!valueType->isArrayValue()) {
        return false;
    }
    auto arrayValue = std::dynamic_pointer_cast<ArrayValue>(valueType);
    return getType()->isEquals(arrayValue->getType()) && values == arrayValue->values;
}

}// namespace NES