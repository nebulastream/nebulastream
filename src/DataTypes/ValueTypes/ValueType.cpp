#include <DataTypes/ValueTypes/ValueType.hpp>
namespace NES{

ValueType::ValueType(DataTypePtr type) : dataType(type){
}

int ValueType::isArrayValue() {
    return false;
}

int ValueType::isBasicValue() {
    return false;
}

DataTypePtr ValueType::getType() {
    return dataType;
}


}