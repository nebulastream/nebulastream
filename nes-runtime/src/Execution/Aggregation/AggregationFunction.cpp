/*
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

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Runtime::Execution::Aggregation {
AggregationFunction::AggregationFunction(PhysicalTypePtr inputType,
                                         PhysicalTypePtr resultType,
                                         Expressions::ExpressionPtr inputExpression,
                                         Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier)
    : inputType(std::move(inputType)), resultType(std::move(resultType)), inputExpression(std::move(inputExpression)),
      resultFieldIdentifier(std::move(resultFieldIdentifier)) {}

Nautilus::Value<> AggregationFunction::loadFromMemref(Nautilus::Value<Nautilus::MemRef> memref,
                                                      const PhysicalTypePtr& physicalType) {
    if (physicalType->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::INT_8: {
                return memref.load<Nautilus::Int8>();
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return memref.load<Nautilus::Int16>();
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return memref.load<Nautilus::Int32>();
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return memref.load<Nautilus::Int64>();
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return memref.load<Nautilus::UInt8>();
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return memref.load<Nautilus::UInt16>();
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return memref.load<Nautilus::UInt32>();
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return memref.load<Nautilus::UInt64>();
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return memref.load<Nautilus::Float>();
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return memref.load<Nautilus::Double>();
            };
            default: {
                std::stringstream type;
                type << physicalType;
                NES_ERROR("Aggregation Function::load: Physical Type: {} is currently not supported", type.str());
                NES_NOT_IMPLEMENTED();
            };
        }
    } else {
        std::stringstream typeAsString;
        typeAsString << physicalType;
        NES_ERROR("Aggregation Function::load: Physical Type: {} is not a basic type and is currently not supported",
                  typeAsString.str());
        NES_NOT_IMPLEMENTED();
    }
}

Nautilus::Value<> AggregationFunction::createMinValue(const PhysicalTypePtr& physicalType) {
    if (physicalType->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::INT_8: {
                return Nautilus::Value<Nautilus::Int8>(std::numeric_limits<int8_t>::min());
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return Nautilus::Value<Nautilus::Int16>(std::numeric_limits<int16_t>::min());
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return Nautilus::Value<Nautilus::Int32>(std::numeric_limits<int32_t>::min());
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return Nautilus::Value<Nautilus::Int64>(std::numeric_limits<int64_t>::min());
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return Nautilus::Value<Nautilus::UInt8>(std::numeric_limits<uint8_t>::min());
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return Nautilus::Value<Nautilus::UInt16>(std::numeric_limits<uint16_t>::min());
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return Nautilus::Value<Nautilus::UInt32>(std::numeric_limits<uint32_t>::min());
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return Nautilus::Value<Nautilus::UInt64>(std::numeric_limits<uint64_t>::min());
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return Nautilus::Value<Nautilus::Float>(std::numeric_limits<float_t>::min());
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return Nautilus::Value<Nautilus::Double>(std::numeric_limits<double_t>::min());
            };
            default: {
                std::stringstream type;
                type << physicalType;
                NES_ERROR("Aggregation Function::createMinValue: Physical Type: {} is currently not supported", type.str());
                NES_NOT_IMPLEMENTED();
            };
        }
    } else {
        std::stringstream typeString;
        typeString << physicalType;
        NES_ERROR("Aggregation Function::createMinValue: Physical Type: {} is not a basic type and is currently not supported",
                  typeString.str());
        NES_NOT_IMPLEMENTED();
    }
}

Nautilus::Value<> AggregationFunction::createMaxValue(const PhysicalTypePtr& physicalType) {
    if (physicalType->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::INT_8: {
                return Nautilus::Value<Nautilus::Int8>(std::numeric_limits<int8_t>::max());
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return Nautilus::Value<Nautilus::Int16>(std::numeric_limits<int16_t>::max());
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return Nautilus::Value<Nautilus::Int32>(std::numeric_limits<int32_t>::max());
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return Nautilus::Value<Nautilus::Int64>(std::numeric_limits<int64_t>::max());
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return Nautilus::Value<Nautilus::UInt8>(std::numeric_limits<uint8_t>::max());
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return Nautilus::Value<Nautilus::UInt16>(std::numeric_limits<uint16_t>::max());
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return Nautilus::Value<Nautilus::UInt32>(std::numeric_limits<uint32_t>::max());
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return Nautilus::Value<Nautilus::UInt64>(std::numeric_limits<uint64_t>::max());
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return Nautilus::Value<Nautilus::Float>(std::numeric_limits<float_t>::max());
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return Nautilus::Value<Nautilus::Double>(std::numeric_limits<double_t>::max());
            };
            default: {
                std::stringstream type;
                type << physicalType;
                NES_ERROR("Aggregation Function::createMaxValue: Physical Type: {} is currently not supported", type.str());
                NES_NOT_IMPLEMENTED();
            };
        }
    } else {
        std::stringstream typeString;
        typeString << physicalType;
        NES_ERROR("Aggregation Function::createMaxValue: Physical Type: {} is not a basic type and is currently not supported",
                  typeString.str());
        NES_NOT_IMPLEMENTED();
    }
}

Nautilus::Value<> AggregationFunction::createConstValue(int64_t value, const PhysicalTypePtr& physicalType) {
    if (physicalType->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::INT_8: {
                return Nautilus::Value<Nautilus::Int8>((int8_t) (value));
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return Nautilus::Value<Nautilus::Int16>((int16_t) (value));
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return Nautilus::Value<Nautilus::Int32>((int32_t) (value));
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return Nautilus::Value<Nautilus::Int64>((int64_t) value);
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return Nautilus::Value<Nautilus::UInt8>((uint8_t) (value));
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return Nautilus::Value<Nautilus::UInt16>((uint16_t) (value));
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return Nautilus::Value<Nautilus::UInt32>((uint32_t) (value));
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return Nautilus::Value<Nautilus::UInt64>((uint64_t) value);
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return Nautilus::Value<Nautilus::Float>((float) (value));
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return Nautilus::Value<Nautilus::Double>((double) (value));
            };
            default: {
                std::stringstream type;
                type << physicalType;
                NES_ERROR("Aggregation Function::load: Physical Type: {} is currently not supported", type.str());
                NES_NOT_IMPLEMENTED();
            };
        }
    } else {
        std::stringstream typeString;
        typeString << physicalType;
        NES_ERROR("Aggregation Function::load: Physical Type: {} is not a basic type and is currently not supported",
                  typeString.str());
        NES_NOT_IMPLEMENTED();
    }
}

AggregationFunction::~AggregationFunction() = default;

}// namespace NES::Runtime::Execution::Aggregation
