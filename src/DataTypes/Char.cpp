
#include <DataTypes/Char.hpp>
#include <DataTypes/DataTypeFactory.hpp>

namespace NES {

Char::Char(uint64_t length) : length(length){}

uint64_t Char::getLength() const {
    return length;
}

bool Char::isChar() {
    return true;
}
bool Char::isEquals(DataTypePtr otherDataType) {
    if(otherDataType->isChar()){
        auto otherChar = as<Char>(otherDataType);
        return length == otherChar->getLength();
    }
    return false;
}

DataTypePtr Char::join(DataTypePtr otherDataType) {
    return DataTypeFactory::createUndefined();
}

}// namespace NES