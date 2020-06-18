#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/ArrayPhysicalType.hpp>
#include <sstream>

namespace NES {

ArrayPhysicalType::ArrayPhysicalType(DataTypePtr type, uint64_t length, PhysicalTypePtr physicalComponentType) : PhysicalType(type), length(length), physicalComponentType(physicalComponentType) {
}

PhysicalTypePtr ArrayPhysicalType::create(DataTypePtr type, uint64_t length, PhysicalTypePtr component) {
    return std::make_shared<ArrayPhysicalType>(type, length, component);
}

bool ArrayPhysicalType::isArrayType() {
    return true;
}

u_int8_t ArrayPhysicalType::size() const {
    return physicalComponentType->size() * length;
}
const PhysicalTypePtr& ArrayPhysicalType::getPhysicalComponentType() const {
    return physicalComponentType;
}
const uint64_t ArrayPhysicalType::getLength() const {
    return length;
}
std::string ArrayPhysicalType::convertRawToString(void* data) {
    std::stringstream str;

    // check if the pointer is valid
    if (!data)
        return "";

    // we print a fixed char directly because the last char terminated the output.
    if (type->isFixedChar()) {
        return static_cast<char*>(data);
    }

    char* pointer = static_cast<char*>(data);

    if (!type->isFixedChar())
        str << '[';
    for (u_int32_t dimension = 0; dimension < length; dimension++) {
        if ((dimension != 0) && !type->isFixedChar())
            str << ", ";
        auto fieldOffset = physicalComponentType->size();
        auto componentValue = &pointer[fieldOffset * dimension];
        str << physicalComponentType->convertRawToString(componentValue);
    }
    if (!type->isFixedChar())
        str << ']';
    return str.str();
}
std::string ArrayPhysicalType::toString() {
    std::stringstream sstream;
    if (type->isFixedChar()) {
        sstream << physicalComponentType->toString();
        return sstream.str();
    } else {
        sstream << physicalComponentType->toString() << "[" << length << "]";
        return sstream.str();
    }
}
}// namespace NES