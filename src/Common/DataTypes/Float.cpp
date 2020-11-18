/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <algorithm>
#include <cmath>
namespace NES {

Float::Float(int8_t bits, double lowerBound, double upperBound) : Numeric(bits), lowerBound(lowerBound), upperBound(upperBound) {}

bool Float::isFloat() { return true; }

bool Float::isEquals(DataTypePtr otherDataType) {
    if (otherDataType->isFloat()) {
        auto otherFloat = as<Float>(otherDataType);
        return bits == otherFloat->bits && lowerBound == otherFloat->lowerBound && upperBound == otherFloat->upperBound;
    }
    return false;
}
double Float::getLowerBound() const { return lowerBound; }
double Float::getUpperBound() const { return upperBound; }
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

std::string Float::toString() { return "(Float)"; }

}// namespace NES