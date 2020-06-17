
#include <DataTypes/PhysicalTypes/PhysicalType.hpp>
namespace NES{

PhysicalType::PhysicalType(DataTypePtr type): type(type){}

bool PhysicalType::isArrayType() {
    return false;
}

bool PhysicalType::isBasicType() {
    return false;
}

DataTypePtr PhysicalType::getType() {
    return type;
}



}