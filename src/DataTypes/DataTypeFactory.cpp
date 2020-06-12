
#include <DataTypes/Array.hpp>
#include <DataTypes/Boolean.hpp>
#include <DataTypes/Char.hpp>
#include <DataTypes/DataTypeFactory.hpp>
#include <DataTypes/Float.hpp>
#include <DataTypes/Integer.hpp>
#include <DataTypes/Undefined.hpp>
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
    auto bits = lowerBound >= std::numeric_limits<float>::min()  && upperBound <= std::numeric_limits<float>::min() ? 32 : 64;
    createFloat(bits, lowerBound, upperBound);
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

DataTypePtr DataTypeFactory::createChar(uint64_t length) {
    return std::make_shared<Char>(length);
}

}// namespace NES