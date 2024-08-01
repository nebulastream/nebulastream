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

void AggregationFunction::storeToMemRef(Nautilus::MemRef memRef, const Nautilus::ExecDataType& execValue, const PhysicalTypePtr& physicalType) {
    if (physicalType->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::INT_8: {
                *static_cast<nautilus::val<int8_t*>>(memRef) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<int8_t>>(execValue)->as<int8_t>();
                break;
            };
            case BasicPhysicalType::NativeType::INT_16: {
                *static_cast<nautilus::val<int16_t*>>(memRef) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<int16_t>>(execValue)->as<int16_t>();
                break;
            };
            case BasicPhysicalType::NativeType::INT_32: {
                *static_cast<nautilus::val<int32_t*>>(memRef) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<int32_t>>(execValue)->as<int32_t>();
                break;
            };
            case BasicPhysicalType::NativeType::INT_64: {
                *static_cast<nautilus::val<int64_t*>>(memRef) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<int64_t>>(execValue)->as<int64_t>();
                break;
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                *static_cast<nautilus::val<uint8_t*>>(memRef) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<uint8_t>>(execValue)->as<uint8_t>();
                break;
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                *static_cast<nautilus::val<uint16_t*>>(memRef) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<uint16_t>>(execValue)->as<uint16_t>();
                break;
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                *static_cast<nautilus::val<uint32_t*>>(memRef) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<uint32_t>>(execValue)->as<uint32_t>();
                break;
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                *static_cast<nautilus::val<uint64_t*>>(memRef) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<uint64_t>>(execValue)->as<uint64_t>();
                break;
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                *static_cast<nautilus::val<float*>>(memRef) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<float>>(execValue)->as<float>();
                break;
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                *static_cast<nautilus::val<double*>>(memRef) =
                    std::dynamic_pointer_cast<Nautilus::ExecutableDataType<double>>(execValue)->as<double>();
                break;
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

Nautilus::ExecDataType AggregationFunction::loadFromMemRef(Nautilus::MemRef memRef, const PhysicalTypePtr& physicalType) {
    if (physicalType->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::INT_8: {
                return Nautilus::ExecutableDataType<int8_t>::create(
                    static_cast<nautilus::val<int8_t>>(*static_cast<nautilus::val<int8_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return Nautilus::ExecutableDataType<int16_t>::create(
                    static_cast<nautilus::val<int16_t>>(*static_cast<nautilus::val<int16_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return Nautilus::ExecutableDataType<int32_t>::create(
                    static_cast<nautilus::val<int32_t>>(*static_cast<nautilus::val<int32_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return Nautilus::ExecutableDataType<int64_t>::create(
                    static_cast<nautilus::val<int64_t>>(*static_cast<nautilus::val<int64_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return Nautilus::ExecutableDataType<uint8_t>::create(
                    static_cast<nautilus::val<uint8_t>>(*static_cast<nautilus::val<uint8_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return Nautilus::ExecutableDataType<uint16_t>::create(
                    static_cast<nautilus::val<uint16_t>>(*static_cast<nautilus::val<uint16_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return Nautilus::ExecutableDataType<uint32_t>::create(
                    static_cast<nautilus::val<uint32_t>>(*static_cast<nautilus::val<uint32_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return Nautilus::ExecutableDataType<uint64_t>::create(
                    static_cast<nautilus::val<uint64_t>>(*static_cast<nautilus::val<uint64_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return Nautilus::ExecutableDataType<float>::create(
                    static_cast<nautilus::val<float>>(*static_cast<nautilus::val<float*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return Nautilus::ExecutableDataType<double>::create(
                    static_cast<nautilus::val<double>>(*static_cast<nautilus::val<double*>>(memRef)));
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

Nautilus::ExecDataType AggregationFunction::createMinValue(const PhysicalTypePtr& physicalType) {
    if (physicalType->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::INT_8: {
                return Nautilus::ExecutableDataType<int8_t>::create(std::numeric_limits<int8_t>::min());
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return Nautilus::ExecutableDataType<int16_t>::create(std::numeric_limits<int16_t>::min());
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return Nautilus::ExecutableDataType<int32_t>::create(std::numeric_limits<int32_t>::min());
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return Nautilus::ExecutableDataType<int64_t>::create(std::numeric_limits<int64_t>::min());
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return Nautilus::ExecutableDataType<uint8_t>::create(std::numeric_limits<uint8_t>::min());
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return Nautilus::ExecutableDataType<uint16_t>::create(std::numeric_limits<uint16_t>::min());
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return Nautilus::ExecutableDataType<uint32_t>::create(std::numeric_limits<uint32_t>::min());
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return Nautilus::ExecutableDataType<uint64_t>::create(std::numeric_limits<uint64_t>::min());
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return Nautilus::ExecutableDataType<float_t>::create(std::numeric_limits<float_t>::min());
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return Nautilus::ExecutableDataType<double_t>::create(std::numeric_limits<double_t>::min());
            };
            default: {
                std::stringstream type;
                type << physicalType;
                NES_ERROR("Aggregation Function::createMinexecValue: Physical Type: {} is currently not supported", type.str());
                NES_NOT_IMPLEMENTED();
            };
        }
    } else {
        std::stringstream typeString;
        typeString << physicalType;
        NES_ERROR("Aggregation Function::createMinexecValue: Physical Type: {} is not a basic type and is currently not supported",
                  typeString.str());
        NES_NOT_IMPLEMENTED();
    }
}

Nautilus::ExecDataType AggregationFunction::createMaxValue(const NES::PhysicalTypePtr& physicalType) {
    if (physicalType->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::INT_8: {
                return Nautilus::ExecutableDataType<int8_t>::create(std::numeric_limits<int8_t>::max());
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return Nautilus::ExecutableDataType<int16_t>::create(std::numeric_limits<int16_t>::max());
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return Nautilus::ExecutableDataType<int32_t>::create(std::numeric_limits<int32_t>::max());
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return Nautilus::ExecutableDataType<int64_t>::create(std::numeric_limits<int64_t>::max());
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return Nautilus::ExecutableDataType<uint8_t>::create(std::numeric_limits<uint8_t>::max());
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return Nautilus::ExecutableDataType<uint16_t>::create(std::numeric_limits<uint16_t>::max());
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return Nautilus::ExecutableDataType<uint32_t>::create(std::numeric_limits<uint32_t>::max());
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return Nautilus::ExecutableDataType<uint64_t>::create(std::numeric_limits<uint64_t>::max());
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return Nautilus::ExecutableDataType<float_t>::create(std::numeric_limits<float_t>::max());
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return Nautilus::ExecutableDataType<double_t>::create(std::numeric_limits<double_t>::max());
            };
            default: {
                std::stringstream type;
                type << physicalType;
                NES_ERROR("Aggregation Function::createMaxexecValue: Physical Type: {} is currently not supported", type.str());
                NES_NOT_IMPLEMENTED();
            };
        }
    } else {
        std::stringstream typeString;
        typeString << physicalType;
        NES_ERROR("Aggregation Function::createMaxexecValue: Physical Type: {} is not a basic type and is currently not supported",
                  typeString.str());
        NES_NOT_IMPLEMENTED();
    }
}

Nautilus::ExecDataType AggregationFunction::createConstValue(int64_t value, const PhysicalTypePtr& physicalType) {
    if (physicalType->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::INT_8: {
                return Nautilus::ExecutableDataType<int8_t>::create(static_cast<int8_t>(value));
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return Nautilus::ExecutableDataType<int16_t>::create(static_cast<int16_t>(value));
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return Nautilus::ExecutableDataType<int32_t>::create(static_cast<int32_t>(value));
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return Nautilus::ExecutableDataType<int64_t>::create(static_cast<int64_t>(value));
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return Nautilus::ExecutableDataType<uint8_t>::create(static_cast<uint8_t>(value));
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return Nautilus::ExecutableDataType<uint16_t>::create(static_cast<uint16_t>(value));
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return Nautilus::ExecutableDataType<uint32_t>::create(static_cast<uint32_t>(value));
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return Nautilus::ExecutableDataType<uint64_t>::create(static_cast<uint64_t>(value));
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return Nautilus::ExecutableDataType<float_t>::create(static_cast<float_t>(value));
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return Nautilus::ExecutableDataType<double_t>::create(static_cast<double_t>(value));
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
