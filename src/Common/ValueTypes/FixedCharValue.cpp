
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ValueTypes/FixedCharValue.hpp>
#include <sstream>
namespace NES {

FixedCharValue::FixedCharValue(std::vector<std::string> values) : ValueType(DataTypeFactory::createFixedChar(values.size())), values(values), isString(false) {}

FixedCharValue::FixedCharValue(const std::string& value) : ValueType(DataTypeFactory::createFixedChar(value.size())), isString(true) {
    std::stringstream str;
    values.push_back(value);
}

bool FixedCharValue::isCharValue() {
    return true;
}

bool FixedCharValue::isEquals(ValueTypePtr) {
    return false;
}

std::string FixedCharValue::toString() {
    return std::string();
}

const std::vector<std::string>& FixedCharValue::getValues() const {
    return values;
}

bool FixedCharValue::getIsString() const {
    return isString;
}

}// namespace NES