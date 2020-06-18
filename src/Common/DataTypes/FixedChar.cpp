
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/FixedChar.hpp>

namespace NES {

FixedChar::FixedChar(uint64_t length) : length(length) {}

uint64_t FixedChar::getLength() const {
    return length;
}

bool FixedChar::isFixedChar() {
    return true;
}
bool FixedChar::isEquals(DataTypePtr otherDataType) {
    if (otherDataType->isFixedChar()) {
        auto otherChar = as<FixedChar>(otherDataType);
        return length == otherChar->getLength();
    }
    return false;
}

DataTypePtr FixedChar::join(DataTypePtr otherDataType) {
    return DataTypeFactory::createUndefined();
}

std::string FixedChar::toString() {
    return "Char";
}

}// namespace NES