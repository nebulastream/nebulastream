
#include <DataTypes/DataTypeFactory.hpp>
#include <DataTypes/Undefined.hpp>
#include <DataTypes/Boolean.hpp>
#include <DataTypes/Char.hpp>
#include <DataTypes/Array.hpp>
#include <DataTypes/Integer.hpp>
#include <DataTypes/Float.hpp>
namespace NES{

DataTypePtr DataTypeFactory::createUndefined() {
    return std::make_shared<Undefined>();
}

DataTypePtr DataTypeFactory::createBoolean() {
    return std::make_shared<Boolean>();
}

DataTypePtr DataTypeFactory::createFloat(int8_t bits, double lowerBound, double upperBound) {
    return std::make_shared<Float>(bits, lowerBound, upperBound);
}

DataTypePtr DataTypeFactory::createInteger(int8_t bits, int64_t lowerBound, int64_t upperBound) {

}

}