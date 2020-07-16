
#include <API/Expressions/Expressions.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <sstream>
namespace NES {

BasicPhysicalType::BasicPhysicalType(DataTypePtr type, NativeType nativeType) : PhysicalType(type), nativeType(nativeType) {}

PhysicalTypePtr BasicPhysicalType::create(DataTypePtr type, NativeType nativeType) {
    return std::make_shared<BasicPhysicalType>(type, nativeType);
}

bool BasicPhysicalType::isBasicType() {
    return true;
}

uint64_t BasicPhysicalType::size() const {
    switch (nativeType) {
        case INT_8: return sizeof(int8_t);
        case INT_16: return sizeof(int16_t);
        case INT_32: return sizeof(int32_t);
        case INT_64: return sizeof(int64_t);
        case UINT_8: return sizeof(uint8_t);
        case UINT_16: return sizeof(uint16_t);
        case UINT_32: return sizeof(uint32_t);
        case UINT_64: return sizeof(uint64_t);
        case FLOAT: return sizeof(float);
        case DOUBLE: return sizeof(double);
        case BOOLEAN: return sizeof(bool);
        case CHAR: return sizeof(char);
    };
}
std::string BasicPhysicalType::convertRawToString(void* data) {

    if (!data)
        return std::string();
    std::stringstream str;
    switch (nativeType) {
        case INT_8: return std::to_string(*reinterpret_cast<int8_t*>(data));
        case UINT_8: return std::to_string(*reinterpret_cast<uint8_t*>(data));
        case INT_16: return std::to_string(*reinterpret_cast<int16_t*>(data));
        case UINT_16: return std::to_string(*reinterpret_cast<uint16_t*>(data));
        case INT_32: return std::to_string(*reinterpret_cast<int32_t*>(data));
        case UINT_32: return std::to_string(*reinterpret_cast<uint32_t*>(data));
        case INT_64: return std::to_string(*reinterpret_cast<int64_t*>(data));
        case UINT_64: return std::to_string(*reinterpret_cast<uint64_t*>(data));
        case FLOAT: return std::to_string(*reinterpret_cast<float*>(data));
        case DOUBLE: return std::to_string(*reinterpret_cast<double*>(data));
        case BOOLEAN: return std::to_string(*reinterpret_cast<bool*>(data));
        case CHAR:
            if (size() == 0) {
                str << static_cast<char>(*static_cast<char*>(data));
            } else {
                auto charData = static_cast<char*>(data);
                // This char is fixed size, so we have to convert it to a fixed size string.
                // Otherwise we would copy all data till the termination character.
                str << std::string(charData, size());
            }
            return str.str();
    }
}

BasicPhysicalType::NativeType BasicPhysicalType::getNativeType() {
    return nativeType;
}
std::string BasicPhysicalType::toString() {
    switch (nativeType) {
        case INT_8: return "INT8";
        case UINT_8: return "UINT8";
        case INT_16: return "INT16";
        case UINT_16: return "UINT16";
        case INT_32: return "INT32";
        case UINT_32: return "UINT32";
        case INT_64: return "INT64";
        case UINT_64: return "UINT64";
        case FLOAT: return "FLOAT32";
        case DOUBLE: return "FLOAT64";
        case BOOLEAN: return "BOOLEAN";
        case CHAR: return "CHAR";
    }
    return "";
}

}// namespace NES
