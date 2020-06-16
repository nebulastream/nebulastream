
#include <DataTypes/Array.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Float.hpp>
#include <DataTypes/Integer.hpp>
#include <DataTypes/PhysicalTypes/BasicPhysicalType.hpp>
#include <DataTypes/PhysicalTypes/ArrayPhysicalType.hpp>
#include <DataTypes/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Util/Logger.hpp>
namespace NES {

DefaultPhysicalTypeFactory::DefaultPhysicalTypeFactory() : PhysicalTypeFactory() {}

PhysicalTypePtr DefaultPhysicalTypeFactory::getPhysicalType(DataTypePtr dataType) {
    if (dataType->isBoolean()) {
        return BasicPhysicalType::create(dataType, BasicPhysicalType::BOOLEAN);
    } else if (dataType->isInteger()) {
        return getPhysicalType(DataType::as<Integer>(dataType));
    } else if (dataType->isFloat()) {
        return getPhysicalType(DataType::as<Float>(dataType));
    } else if (dataType->isArray()) {
        return getPhysicalType(DataType::as<Array>(dataType));
    }
    NES_THROW_RUNTIME_ERROR("DefaultPhysicalTypeFactory: it was not possible to infer a physical type for: " + dataType->toString());
}

PhysicalTypePtr DefaultPhysicalTypeFactory::getPhysicalType(IntegerPtr integer) {
    if (integer->getLowerBound() >= 0) {
        if (integer->getBits() <= 8) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::UINT_8);
        } else if (integer->getBits() <= 16) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::UINT_16);
        } else if (integer->getBits() <= 32) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::UINT_32);
        } else if (integer->getBits() <= 64) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::UINT_64);
        }
    } else {
        if (integer->getBits() <= 8) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::INT_8);
        } else if (integer->getBits() <= 16) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::INT_16);
        } else if (integer->getBits() <= 32) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::INT_32);
        } else if (integer->getBits() <= 64) {
            return BasicPhysicalType::create(integer, BasicPhysicalType::INT_64);
        }
    }
    NES_THROW_RUNTIME_ERROR("DefaultPhysicalTypeFactory: it was not possible to infer a physical type for: " + integer->toString());
}

PhysicalTypePtr DefaultPhysicalTypeFactory::getPhysicalType(FloatPtr floatType) {
    if (floatType->getBits() <= 32) {
        return BasicPhysicalType::create(floatType, BasicPhysicalType::FLOAT);
    } else if (floatType->getBits() <= 64) {
        return BasicPhysicalType::create(floatType, BasicPhysicalType::DOUBLE);
    }
    NES_THROW_RUNTIME_ERROR("DefaultPhysicalTypeFactory: it was not possible to infer a physical type for: " + floatType->toString());
}

PhysicalTypePtr DefaultPhysicalTypeFactory::getPhysicalType(ArrayPtr arrayType) {
    auto componentType = getPhysicalType(arrayType->getComponent());
    return ArrayPhysicalType::create(arrayType, arrayType->getLength(), componentType);
}

}// namespace NES