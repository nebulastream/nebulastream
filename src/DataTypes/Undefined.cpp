
#include <DataTypes/Undefined.hpp>
#include <DataTypes/DataTypeFactory.hpp>

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