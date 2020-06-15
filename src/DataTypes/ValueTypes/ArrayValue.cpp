
#include <DataTypes/ValueTypes/ArrayValueType.hpp>
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

std::vector<std::string> ArrayValue::getValues() {
    return values;
}

}// namespace NES