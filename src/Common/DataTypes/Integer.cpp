#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <algorithm>
namespace NES {

Integer::Integer(int8_t bits, int64_t lowerBound, int64_t upperBound) : Numeric(bits), lowerBound(lowerBound), upperBound(upperBound) {
}

bool Integer::isInteger() {
    return true;
}

bool Integer::isEquals(DataTypePtr otherDataType) {
    if (otherDataType->isInteger()) {
        auto otherInteger = as<Integer>(otherDataType);
        return bits == otherInteger->bits && lowerBound == otherInteger->lowerBound && upperBound == otherInteger->upperBound;
    }
    return false;
}

DataTypePtr Integer::join(DataTypePtr otherDataType) {
    // An integer can be joined with integer types and float types.
    if (otherDataType->isFloat()) {
        // The other type is an float, thus we return a large enough float as a jointed type.
        auto otherFloat = as<Float>(otherDataType);
        auto newBits = std::max(bits, otherFloat->getBits());
        auto newUpperBound = std::max((double) upperBound, otherFloat->getUpperBound());
        auto newLowerBound = std::min((double) lowerBound, otherFloat->getLowerBound());
        return DataTypeFactory::createFloat(newBits, newLowerBound, newUpperBound);
    } else if (otherDataType->isInteger()) {
        // The other type is an Integer, thus we return a large enough integer.
        auto otherInteger = as<Integer>(otherDataType);
        auto newBits = std::max(bits, otherInteger->getBits());
        auto newUpperBound = std::max(upperBound, otherInteger->getUpperBound());
        auto newLowerBound = std::min(lowerBound, otherInteger->getLowerBound());
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
std::string Integer::toString() {
    return "INTEGER";
}

}// namespace NES