#include <DataTypes/PhysicalTypes/ArrayPhysicalType.hpp>

namespace NES {

bool ArrayPhysicalType::isArrayType() {
    return true;
}

u_int8_t ArrayPhysicalType::size() const {
    return ARRAY_HEADER_SIZE + physicalComponentType->size() * length;
}
const PhysicalTypePtr& ArrayPhysicalType::getPhysicalComponentType() const {
    return physicalComponentType;
}
const uint64_t ArrayPhysicalType::getLength() const {
    return length;
}
}