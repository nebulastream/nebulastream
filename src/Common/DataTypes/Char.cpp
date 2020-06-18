
#include <Common/DataTypes/Char.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES {

bool Char::isChar() {
    return true;
}

bool Char::isEquals(DataTypePtr otherDataType) {
    return otherDataType->isChar();
}
DataTypePtr Char::join(DataTypePtr otherDataType) {
    if (otherDataType->isChar()) {
        return DataTypeFactory::createChar();
    }
    return DataTypeFactory::createUndefined();
}

std::string Char::toString() {
    return "Char";
}

}// namespace NES