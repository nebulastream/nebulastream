#include <DataTypes/DataTypeFactory.hpp>
#include <DataTypes/Float.hpp>
#include <DataTypes/Integer.hpp>
#include <algorithm>
namespace NES {

Integer::Integer(int8_t bits, int64_t lowerBound, int64_t upperBound) : Numeric(bits), lowerBound(lowerBound), upperBound(upperBound) {
}

bool Integer::isInteger() {
    return true;
}

bool Integer::isEquals(DataTypePtr otherDataType) {
    if (otherDataType->isInteger()) {
        auto otherFloat = as<Integer>(otherDataType);
        return bits == otherFloat->bits && lowerBound == otherFloat->lowerBound && upperBound == otherFloat->upperBound;
    }
    return false;
}

DataTypePtr Integer::join(DataTypePtr otherDataType) {
    if (otherDataType->isFloat()) {
        auto otherFloat = as<Float>(otherDataType);
        auto newBits = std::max(bits, otherFloat->getBits());
        auto newUpperBound = std::min((double) upperBound, otherFloat->getUpperBound());
        auto newLowerBound = std::max((double) lowerBound, otherFloat->getLowerBound());
        return DataTypeFactory::createFloat(newBits, newLowerBound, newUpperBound);
    } else if (otherDataType->isInteger()) {
        auto otherInteger = as<Integer>(otherDataType);
        auto newBits = std::max(bits, otherInteger->getBits());
        auto newUpperBound = std::min(upperBound, otherInteger->getUpperBound());
        auto newLowerBound = std::max(lowerBound, otherInteger->getLowerBound());
        return DataTypeFactory::createInteger(newBits, newLowerBound, newUpperBound);
    }
    return DataTypeFactory::createUndefined();
}
int64_t Integer::getLowerBound() const {
    return lowerBound;
}
int64_t Integer::getUpperBound() const {
    return upperBound;
}

}// namespace NES