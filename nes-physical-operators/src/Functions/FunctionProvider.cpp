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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/ConstantValuePhysicalFunction.hpp>
#include <Functions/ConstantValueVariableSizePhysicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/VariableSizedDataPhysicalType.hpp>

namespace NES::QueryCompilation
{
PhysicalFunction FunctionProvider::lowerFunction(LogicalFunction logicalFunction)
{
    /// 1. Recursively lower the children of the function node.
    std::vector<PhysicalFunction> childFunction;
    for (auto& child : logicalFunction.getChildren())
    {
        childFunction.emplace_back(lowerFunction(child));
    }

    /// 2. The field access and constant value nodes are special as they require a different treatment,
    /// due to them not simply getting a childFunction as a parameter.
    if (const auto fieldAccessNode = logicalFunction.tryGet<FieldAccessLogicalFunction>())
    {
        return FieldAccessPhysicalFunction(fieldAccessNode->getFieldName());
    }
    if (auto constantValueNode = logicalFunction.tryGet<ConstantValueLogicalFunction>())
    {
        return lowerConstantFunction(*constantValueNode);
    }

    /// 3. Calling the registry to create an executable function.
    auto executableFunctionArguments = PhysicalFunctionRegistryArguments(childFunction);
    if (const auto function
        = PhysicalFunctionRegistry::instance().create(std::string(logicalFunction.getType()), std::move(executableFunctionArguments)))
    {
        return function.value();
    }
    throw UnknownFunctionType("Can not lower function: {}", logicalFunction);
}

PhysicalFunction FunctionProvider::lowerConstantFunction(const ConstantValueLogicalFunction& constantFunction)
{
    const auto stringValue = constantFunction.getConstantValue();
    const auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(constantFunction.getDataType());
    if (auto* const basicType = dynamic_cast<BasicPhysicalType*>(physicalType.get()))
    {
        switch (basicType->nativeType)
        {
            case BasicPhysicalType::NativeType::UINT_8: {
                auto intValue = static_cast<uint8_t>(std::stoul(stringValue));
                return ConstantUInt8ValueFunction(intValue);
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                auto intValue = static_cast<uint16_t>(std::stoul(stringValue));
                return ConstantUInt16ValueFunction(intValue);
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                auto intValue = static_cast<uint32_t>(std::stoul(stringValue));
                return ConstantUInt32ValueFunction(intValue);
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                auto intValue = static_cast<uint64_t>(std::stoull(stringValue));
                return ConstantUInt64ValueFunction(intValue);
            };
            case BasicPhysicalType::NativeType::INT_8: {
                auto intValue = static_cast<int8_t>(std::stoi(stringValue));
                return ConstantInt8ValueFunction(intValue);
            };
            case BasicPhysicalType::NativeType::INT_16: {
                auto intValue = static_cast<int16_t>(std::stoi(stringValue));
                return ConstantInt16ValueFunction(intValue);
            };
            case BasicPhysicalType::NativeType::INT_32: {
                auto intValue = static_cast<int32_t>(std::stoi(stringValue));
                return ConstantInt32ValueFunction(intValue);
            };
            case BasicPhysicalType::NativeType::INT_64: {
                auto intValue = static_cast<int64_t>(std::stol(stringValue));
                return ConstantInt64ValueFunction(intValue);
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                auto floatValue = std::stof(stringValue);
                return ConstantFloatValueFunction(floatValue);
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                auto doubleValue = std::stod(stringValue);
                return ConstantDoubleValueFunction(doubleValue);
            };
            case BasicPhysicalType::NativeType::CHAR:
                break;
            case BasicPhysicalType::NativeType::BOOLEAN: {
                auto boolValue = static_cast<int>(static_cast<bool>(std::stoi(stringValue))) == 1;
                return ConstantBooleanValueFunction(boolValue);
            };
            default: {
                throw UnknownPhysicalType("the basic type {} is not supported", basicType->toString());
            }
        }
    }
    else if (dynamic_cast<VariableSizedDataPhysicalType*>(physicalType.get()) != nullptr)
    {
        return ConstantValueVariableSizePhysicalFunction(reinterpret_cast<const int8_t*>(stringValue.c_str()), stringValue.size());
    }
    throw UnknownPhysicalType("couldn't create ConstantValueFunction for: {}, not a BasicPhysicalType.", physicalType->toString());
}
}
