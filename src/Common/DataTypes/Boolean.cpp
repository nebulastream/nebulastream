
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES {

bool Boolean::isBoolean() {
    return true;
}

bool Boolean::isEquals(DataTypePtr otherDataType) {
    return otherDataType->isBoolean();
}
DataTypePtr Boolean::join(DataTypePtr otherDataType) {
    if (otherDataType->isBoolean()) {
        return DataTypeFactory::createBoolean();
    }
    return DataTypeFactory::createUndefined();
}

std::string Boolean::toString() {
    return "Boolean";
}

}// namespace NES