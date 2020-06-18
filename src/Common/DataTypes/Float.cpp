#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <algorithm>

#include <cmath>
namespace NES {

bool Float::isFloat() {
    return true;
}
Float::Float(int8_t bits, double lowerBound, double upperBound) : Numeric(bits),
                                                                  lowerBound(lowerBound),
                                                                  upperBound(upperBound) {
}

bool Float::isEquals(DataTypePtr otherDataType) {
    if (otherDataType->isFloat()) {
        auto otherFloat = as<Float>(otherDataType);
        return bits == otherFloat->bits && lowerBound == otherFloat->lowerBound && upperBound == otherFloat->upperBound;
    }
    return false;
}
double Float::getLowerBound() const {
    return lowerBound;
}
double Float::getUpperBound() const {
    return upperBound;
}
DataTypePtr Float::join(DataTypePtr otherDataType) {
    if (otherDataType->isFloat()) {
        auto otherFloat = as<Float>(otherDataType);
        auto newBits = std::max(bits, otherFloat->getBits());
        auto newUpperBound = fmax(upperBound, otherFloat->getUpperBound());
        auto newLowerBound = fmin(lowerBound, otherFloat->getLowerBound());
        return DataTypeFactory::createFloat(newBits, newLowerBound, newUpperBound);
    } else if (otherDataType->isInteger()) {
        auto otherInteger = as<Integer>(otherDataType);
        auto newBits = std::max(bits, otherInteger->getBits());
        auto newUpperBound = fmax(upperBound, (double) otherInteger->getUpperBound());
        auto newLowerBound = fmin(lowerBound, (double) otherInteger->getLowerBound());
        return DataTypeFactory::createFloat(newBits, newLowerBound, newUpperBound);
    }
    return DataTypeFactory::createUndefined();
}
bool Float::isNumeric() {
    return true;
}
std::string Float::toString() {
    return "(Float)";
}

}// namespace NES