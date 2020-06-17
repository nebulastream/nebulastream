
#include <DataTypes/Array.hpp>
#include <DataTypes/Boolean.hpp>
#include <DataTypes/DataTypeFactory.hpp>
#include <DataTypes/FixedChar.hpp>
#include <DataTypes/Float.hpp>
#include <DataTypes/Char.hpp>
#include <DataTypes/Integer.hpp>
#include <DataTypes/Undefined.hpp>
#include <DataTypes/ValueTypes/ArrayValueType.hpp>
#include <DataTypes/ValueTypes/BasicValue.hpp>
#include <DataTypes/ValueTypes/CharValueType.hpp>
namespace NES {

DataTypePtr DataTypeFactory::createUndefined() {
    return std::make_shared<Undefined>();
}

DataTypePtr DataTypeFactory::createBoolean() {
    return std::make_shared<Boolean>();
}

DataTypePtr DataTypeFactory::createFloat(int8_t bits, double lowerBound, double upperBound) {
    return std::make_shared<Float>(bits, lowerBound, upperBound);
}

DataTypePtr DataTypeFactory::createFloat() {
    return createFloat(32, std::numeric_limits<float>::min(), std::numeric_limits<float>::max());
}

DataTypePtr DataTypeFactory::createFloat(double lowerBound, double upperBound) {
    auto bits = lowerBound >= std::numeric_limits<float>::min() && upperBound <= std::numeric_limits<float>::min() ? 32 : 64;
    return createFloat(bits, lowerBound, upperBound);
}

DataTypePtr DataTypeFactory::createDouble() {
    return createFloat(64, std::numeric_limits<double>::min(), std::numeric_limits<double>::max());
}

DataTypePtr DataTypeFactory::createInteger(int8_t bits, int64_t lowerBound, int64_t upperBound) {
    return std::make_shared<Integer>(bits, lowerBound, upperBound);
}

DataTypePtr DataTypeFactory::createInteger(int64_t lowerBound, int64_t upperBound) {
    auto bits = upperBound <= INT16_MAX ? 16 : upperBound <= INT32_MAX ? 32 : 64;
    return createInteger(bits, lowerBound, upperBound);
}

DataTypePtr DataTypeFactory::createInt8() {
    return createInteger(8, INT8_MIN, INT8_MAX);
}

DataTypePtr DataTypeFactory::createUInt8() {
    return createInteger(8, 0, UINT8_MAX);
};

DataTypePtr DataTypeFactory::createInt16() {
    return createInteger(16, INT16_MIN, INT16_MAX);
};

DataTypePtr DataTypeFactory::createUInt16() {
    return createInteger(16, 0, UINT16_MAX);
};

DataTypePtr DataTypeFactory::createInt64() {
    return createInteger(64, INT64_MIN, INT64_MAX);
};

DataTypePtr DataTypeFactory::createUInt64() {
    return createInteger(64, 0, UINT64_MAX);
};

DataTypePtr DataTypeFactory::createInt32() {
    return createInteger(32, INT32_MIN, INT32_MAX);
};

DataTypePtr DataTypeFactory::createUInt32() {
    return createInteger(32, 0, UINT32_MAX);
};

DataTypePtr DataTypeFactory::createArray(uint64_t length, DataTypePtr component) {
    return std::make_shared<Array>(length, component);
}

DataTypePtr DataTypeFactory::createFixedChar(uint64_t length) {
    return std::make_shared<FixedChar>(length);
}

DataTypePtr DataTypeFactory::createChar() {
    return std::make_shared<Char>();
}

ValueTypePtr DataTypeFactory::createBasicValue(DataTypePtr type, std::string value) {
    return std::make_shared<BasicValue>(type, value);
}

ValueTypePtr DataTypeFactory::createArrayValue(DataTypePtr type, std::vector<std::string> values) {
    return std::make_shared<ArrayValue>(type, values);
}

ValueTypePtr DataTypeFactory::createCharValue(std::vector<std::string> values) {
    return std::make_shared<CharValueType>(values);
}
ValueTypePtr DataTypeFactory::createCharValue(const char* val) {
    return std::make_shared<CharValueType>(val);
}

DataTypePtr DataTypeFactory::createType(BasicType type) {
    switch (type) {
        case BOOLEAN: return DataTypeFactory::createBoolean();
        case CHAR: return DataTypeFactory::createChar();
        case INT8: return DataTypeFactory::createInt8();
        case INT16: return DataTypeFactory::createInt16();
        case INT32: return DataTypeFactory::createInt32();
        case INT64: return DataTypeFactory::createInt64();
        case UINT8: return DataTypeFactory::createUInt8();
        case UINT16: return DataTypeFactory::createUInt16();
        case UINT32: return DataTypeFactory::createUInt32();
        case UINT64: return DataTypeFactory::createUInt64();
        case FLOAT32: return DataTypeFactory::createFloat();
        case FLOAT64: return DataTypeFactory::createDouble();
    }
}

}// namespace NES