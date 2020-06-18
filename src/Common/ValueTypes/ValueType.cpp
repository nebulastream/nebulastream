#include <Common/ValueTypes/ValueType.hpp>
namespace NES {

ValueType::ValueType(DataTypePtr type) : dataType(type) {
}

bool ValueType::isArrayValue() {
    return false;
}

bool ValueType::isBasicValue() {
    return false;
}

bool ValueType::isCharValue() {
    return false;
}

DataTypePtr ValueType::getType() {
    return dataType;
}

}// namespace NES