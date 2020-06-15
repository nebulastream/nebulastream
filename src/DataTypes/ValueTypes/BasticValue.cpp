
#include <DataTypes/ValueTypes/BasicValue.hpp>
namespace NES{

BasicValue::BasicValue(DataTypePtr type, std::string value) : ValueType(type), value(value){}

bool BasicValue::isBasicValue() {
    return true;
}

std::string BasicValue::toString() {
    return "BasicValue";
}

std::string BasicValue::getValue() {
    return value;
}




}