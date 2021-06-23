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

#include <Common/DataTypes/ArrayType.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/Char.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/FixedChar.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/Undefined.hpp>
#include <Common/ValueTypes/ArrayValue.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Util/Logger.hpp>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <limits>
#include <utility>

namespace NES {

DataTypePtr DataTypeFactory::createUndefined() { return std::make_shared<Undefined>(); }

DataTypePtr DataTypeFactory::createBoolean() { return std::make_shared<Boolean>(); }

DataTypePtr DataTypeFactory::createFloat(int8_t bits, double lowerBound, double upperBound) {
    return std::make_shared<Float>(bits, lowerBound, upperBound);
}

DataTypePtr DataTypeFactory::createFloat() {
    return createFloat(32, std::numeric_limits<float>::max() * -1, std::numeric_limits<float>::max());
}

DataTypePtr DataTypeFactory::createFloat(double lowerBound, double upperBound) {
    auto bits = lowerBound >= std::numeric_limits<float>::max() * -1 && upperBound <= std::numeric_limits<float>::min() ? 32 : 64;
    return createFloat(bits, lowerBound, upperBound);
}

DataTypePtr DataTypeFactory::createDouble() {
    return createFloat(64, std::numeric_limits<double>::max() * -1, std::numeric_limits<double>::max());
}

DataTypePtr DataTypeFactory::createInteger(int8_t bits, int64_t lowerBound, int64_t upperBound) {
    return std::make_shared<Integer>(bits, lowerBound, upperBound);
}

DataTypePtr DataTypeFactory::createInteger(int64_t lowerBound, int64_t upperBound) {
    // derive the correct bite size for the correct lower and upper bound
    auto bits = upperBound <= INT8_MAX ? 8 : upperBound <= INT16_MAX ? 16 : upperBound <= INT32_MAX ? 32 : 64;
    return createInteger(bits, lowerBound, upperBound);
}

DataTypePtr DataTypeFactory::createInt8() { return createInteger(8, INT8_MIN, INT8_MAX); }

DataTypePtr DataTypeFactory::createUInt8() { return createInteger(8, 0, UINT8_MAX); };

DataTypePtr DataTypeFactory::createInt16() { return createInteger(16, INT16_MIN, INT16_MAX); };

DataTypePtr DataTypeFactory::createUInt16() { return createInteger(16, 0, UINT16_MAX); };

DataTypePtr DataTypeFactory::createInt64() { return createInteger(64, INT64_MIN, INT64_MAX); };

DataTypePtr DataTypeFactory::createUInt64() {
    return createInteger(64, 0, UINT64_MAX);
};// TODO / BUG: upper bound is a INT64 and can not capture this upper bound. -> upperbound overflows and is set to -1.

DataTypePtr DataTypeFactory::createInt32() { return createInteger(32, INT32_MIN, INT32_MAX); };

DataTypePtr DataTypeFactory::createUInt32() { return createInteger(32, 0, UINT32_MAX); };

DataTypePtr DataTypeFactory::createArray(uint64_t length, const DataTypePtr& component) {
    return std::make_shared<ArrayType>(length, component);
}

DataTypePtr DataTypeFactory::createFixedChar(uint64_t length) { return std::make_shared<FixedChar>(length); }

DataTypePtr DataTypeFactory::createChar() { return std::make_shared<Char>(); }

ValueTypePtr DataTypeFactory::createBasicValue(DataTypePtr type, std::string value) {
    return std::make_shared<BasicValue>(std::move(type), std::move(value));
}

ValueTypePtr DataTypeFactory::createBasicValue(uint64_t value) { return createBasicValue(createUInt64(), std::to_string(value)); }

ValueTypePtr DataTypeFactory::createBasicValue(int64_t value) { return createBasicValue(createInt64(), std::to_string(value)); }

ValueTypePtr DataTypeFactory::createBasicValue(BasicType type, std::string value) {
    return createBasicValue(createType(type), std::move(value));
}

ValueTypePtr DataTypeFactory::createArrayValueFromContainerType(std::shared_ptr<ArrayType>&& type,
                                                                std::vector<std::string>&& values) noexcept {

    return std::make_shared<ArrayValue>(std::move(type), std::move(values));
}

ValueTypePtr DataTypeFactory::createArrayValueWithContainedType(DataTypePtr&& type, std::vector<std::string>&& values) noexcept {
    auto const length = values.size();
    return std::make_shared<ArrayValue>(createArray(length, std::move(type)), std::move(values));
}

ValueTypePtr DataTypeFactory::createFixedCharValue(std::string&& values) noexcept { return createFixedCharValue(values.c_str()); }

ValueTypePtr DataTypeFactory::createFixedCharValue(std::string const& values) noexcept {
    return createFixedCharValue(values.c_str());
}

ValueTypePtr DataTypeFactory::createFixedCharValue(std::vector<std::string>&& values) noexcept {
    return createArrayValueWithContainedType(createChar(), std::move(values));
}

ValueTypePtr DataTypeFactory::createFixedCharValue(char const* values) noexcept {
    std::vector<std::string> vec{};
    auto const size = strlen(values) + 1;
    vec.reserve(size);
    // Copy string including string termination character ( which is legal this way :) ).
    for (std::size_t s = 0; s < size; ++s) {
        vec.push_back(std::string{values[s]});
    }
    assert(vec.size() == size);
    return createFixedCharValue(std::move(vec));
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
        default: return nullptr;
    }
    return DataTypePtr();
}

DataTypePtr DataTypeFactory::copyTypeAndIncreaseLowerBound(DataTypePtr stamp, double minLowerBound) {
    if (stamp->isFloat()) {
        if (auto const floatStamp = DataType::as<Float>(stamp); floatStamp->lowerBound < minLowerBound) {
            return createFloat(floatStamp->getBits(), minLowerBound, floatStamp->upperBound);
        }
    } else if (stamp->isInteger()) {
        if (auto const intStamp = DataType::as<Integer>(stamp); intStamp->lowerBound < minLowerBound) {
            NES_WARNING("DataTypeFactory: A Float is passed as the minimum lower bound of an Integer data type. Will be executed "
                        "with the Floor of the Float argument instead.");
            return createInteger(intStamp->getBits(), static_cast<int64_t>(minLowerBound), intStamp->upperBound);
        }
    } else {
        // non-numeric data types do not have a lower bound
        NES_ERROR("DataTypeFactory: Can not increase a lower bound on a non-numeric data type.");
    }
    return stamp;// increase does not apply -> return shared pointer given as argument
}

DataTypePtr DataTypeFactory::copyTypeAndIncreaseLowerBound(DataTypePtr stamp, int64_t minLowerBound) {
    if (stamp->isInteger()) {
        if (auto const intStamp = DataType::as<Integer>(stamp); intStamp->lowerBound < minLowerBound) {
            return createInteger(intStamp->getBits(), minLowerBound, intStamp->upperBound);
        }
    } else if (stamp->isFloat()) {
        if (auto const floatStamp = DataType::as<Float>(stamp); floatStamp->lowerBound < minLowerBound) {
            NES_INFO("DataTypeFactory: An Integer is passed as the minimum lower bound of a Float data type. Progresses with "
                     "standard casting to Double.");
            return createFloat(floatStamp->getBits(), static_cast<double>(minLowerBound), floatStamp->upperBound);
        }
    } else {
        // non-numeric data types do not have a lower bound
        NES_ERROR("DataTypeFactory: Can not increase a lower bound on a non-numeric data type.");
    }
    NES_INFO("DataTypeFactory: Increase of lower bound does not apply. Returning original stamp.");
    return stamp;// increase does not apply -> return shared pointer given as argument
}

DataTypePtr DataTypeFactory::copyTypeAndDecreaseUpperBound(DataTypePtr stamp, double maxUpperBound) {
    if (stamp->isFloat()) {
        if (auto const floatStamp = DataType::as<Float>(stamp); maxUpperBound < floatStamp->upperBound) {
            return createFloat(floatStamp->getBits(), floatStamp->lowerBound, maxUpperBound);
        }
    } else if (stamp->isInteger()) {
        if (auto const intStamp = DataType::as<Integer>(stamp); maxUpperBound < intStamp->upperBound) {
            NES_WARNING("DataTypeFactory: A Float is passed as the maximum upper bound of an Integer data type. Progresses with "
                        "the Ceiling of the Float argument instead.");
            return createInteger(intStamp->getBits(), intStamp->lowerBound, static_cast<int64_t>(std::ceil(maxUpperBound)));
        }
    } else {
        // non-numeric data types do not have a lower bound
        NES_ERROR("DataTypeFactory: Can not increase a lower bound on a non-numeric data type.");
    }
    NES_INFO("DataTypeFactory: Decrease of upper bound does not apply. Returning original stamp.");
    return stamp;// decrease does not apply -> return shared pointer given as argument
}

DataTypePtr DataTypeFactory::copyTypeAndDecreaseUpperBound(DataTypePtr stamp, int64_t maxUpperBound) {
    if (stamp->isInteger()) {
        if (auto const intStamp = DataType::as<Integer>(stamp); maxUpperBound < intStamp->upperBound) {
            return createInteger(intStamp->getBits(), intStamp->lowerBound, maxUpperBound);
        }
    } else if (stamp->isFloat()) {
        if (auto const floatStamp = DataType::as<Float>(stamp); maxUpperBound < floatStamp->upperBound) {
            NES_INFO("DataTypeFactory: An Integer is passed as the maximum upper bound of an Float data type. Progresses with "
                     "standard casting to Double.");
            return createFloat(floatStamp->getBits(), floatStamp->lowerBound, static_cast<double>(maxUpperBound));
        }
    } else {
        // non-numeric data types do not have a lower bound
        NES_ERROR("DataTypeFactory: Can not increase a lower bound on a non-numeric data type.");
    }
    NES_INFO("DataTypeFactory: Decrease of upper bound does not apply. Returning original stamp.");
    return stamp;// decrease does not apply -> return shared pointer given as argument
}

DataTypePtr DataTypeFactory::copyTypeAndTightenBounds(DataTypePtr stamp, int64_t minLowerBound, int64_t maxUpperBound) {
    if (stamp->isInteger()) {
        if (auto const intStamp = DataType::as<Integer>(stamp); intStamp->lowerBound < minLowerBound) {
            // we must create a new stamp for an increased lower bound, so we calculate the upper bound by predication
            int64_t newUpperBound = std::min(intStamp->upperBound, maxUpperBound);
            return createInteger(intStamp->getBits(), minLowerBound, newUpperBound);
        } else if (maxUpperBound < intStamp->upperBound) {
            // we must create a new stamp but keep the old lower bound
            return createInteger(intStamp->getBits(), intStamp->lowerBound, maxUpperBound);
        }
    } else if (stamp->isFloat()) {
        NES_INFO("DataTypeFactory: Integers are passed as new bounds of an Float data type. Progresses with standard casting to "
                 "Double.");
        return DataTypeFactory::copyTypeAndTightenBounds(stamp,
                                                         static_cast<double>(minLowerBound),
                                                         static_cast<double>(maxUpperBound));
    } else {
        // non-numeric data types do not have a lower bound
        NES_ERROR("DataTypeFactory: Can not modify bounds on a non-numeric data type.");
    }
    NES_INFO("DataTypeFactory: Lower and upper bound do not need to be changed. Returning original stamp.");
    return stamp;// neither bound needs to be modified -> return shared pointer given as argument
}

DataTypePtr DataTypeFactory::copyTypeAndTightenBounds(DataTypePtr stamp, double minLowerBound, double maxUpperBound) {
    if (stamp->isFloat()) {
        if (auto const floatStamp = DataType::as<Float>(stamp); floatStamp->lowerBound < minLowerBound) {
            // we must create a new stamp for an increased lower bound, so we calculate the upper bound by predication
            double newUpperBound = fmin(floatStamp->upperBound, maxUpperBound);
            return createFloat(floatStamp->getBits(), minLowerBound, newUpperBound);
        } else if (maxUpperBound < floatStamp->upperBound) {
            // we must create a new stamp but keep the old lower bound
            return createFloat(floatStamp->getBits(), floatStamp->lowerBound, maxUpperBound);
        }
    } else if (stamp->isInteger()) {
        NES_INFO("DataTypeFactory: Floats are passed as new bounds of an Integer data type. Progresses with the floor of the "
                 "lower bound and ceiling of the upper bound instead.");
        return DataTypeFactory::copyTypeAndTightenBounds(stamp,
                                                         static_cast<int64_t>(std::floor(minLowerBound)),
                                                         static_cast<int64_t>(std::ceil(maxUpperBound)));
    } else {
        // non-numeric data types do not have a lower bound
        NES_ERROR("DataTypeFactory: Can not modify bounds on a non-numeric data type.");
    }
    NES_INFO("DataTypeFactory: Lower and upper bound do not need to be changed. Returning original stamp.");
    return stamp;// neither bound needs to be modified -> return shared pointer given as argument
}

DataTypePtr DataTypeFactory::createFloatFromInteger(DataTypePtr stamp) {
    if (stamp->isInteger()) {
        auto const intStamp = DataType::as<Integer>(stamp);
        return DataTypeFactory::createFloat(intStamp->lowerBound, intStamp->upperBound);
    } else if (stamp->isFloat()) {
        NES_INFO("DataTypeFactory: A Float is passed to be converted to a Float. Return stamp passed as argument.");
    } else {
        // call with non-numeric is not allowed
        NES_ERROR("DataTypeFactory: Can not modify bounds on a non-numeric data type.");
    }
    return stamp;
}

}// namespace NES