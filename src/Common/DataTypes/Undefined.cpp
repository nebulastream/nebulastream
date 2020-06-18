
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Undefined.hpp>

namespace NES {

bool Undefined::isUndefined() {
    return true;
}

bool Undefined::isEquals(DataTypePtr otherDataType) {
    return otherDataType->isUndefined();
}

DataTypePtr Undefined::join(DataTypePtr otherDataType) {
    return DataTypeFactory::createUndefined();
}
std::string Undefined::toString() {
    return "Undefined";
}

}// namespace NES