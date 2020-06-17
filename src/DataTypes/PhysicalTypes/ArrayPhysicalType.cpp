#include <DataTypes/PhysicalTypes/ArrayPhysicalType.hpp>
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
std::string ArrayPhysicalType::convertRawToString(void* rawData) {
    return "Array String";
}
std::string ArrayPhysicalType::toString() {
    std::stringstream sstream;
    sstream << physicalComponentType->toString() << "[" << length << "]";
    return sstream.str();
}
}