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
#include <Functions/FunctionProvider.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <Functions/CastFieldPhysicalFunction.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/ConstantValuePhysicalFunction.hpp>
#include <Functions/ConstantValueVariableSizePhysicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES::QueryCompilation
{
PhysicalFunction FunctionProvider::lowerFunction(LogicalFunction logicalFunction)
{
    /// 1. Recursively lower the children of the function node.
    std::vector<PhysicalFunction> childFunction;
    for (const auto& child : logicalFunction.getChildren())
    {
        childFunction.emplace_back(lowerFunction(child));
    }

    /// 2. The field access and constant value nodes are special as they require a different treatment,
    /// due to them not simply getting a childFunction as a parameter.
    if (const auto fieldAccessFunction = logicalFunction.tryGet<FieldAccessLogicalFunction>())
    {
        return FieldAccessPhysicalFunction(fieldAccessFunction->getFieldName());
    }
    if (const auto constantValueFunction = logicalFunction.tryGet<ConstantValueLogicalFunction>())
    {
        return lowerConstantFunction(*constantValueFunction);
    }
    if (const auto castToTypeNode = logicalFunction.tryGet<CastToTypeLogicalFunction>())
    {
        INVARIANT(childFunction.size() == 1, "CastFieldPhysicalFunction expects exact one child!");
        return CastFieldPhysicalFunction(childFunction[0], castToTypeNode->getDataType());
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
    switch (constantFunction.getDataType().type)
    {
        case DataType::Type::UINT8: {
            const auto intValue = static_cast<uint8_t>(std::stoul(stringValue));
            return ConstantUInt8ValueFunction(intValue);
        };
        case DataType::Type::UINT16: {
            const auto intValue = static_cast<uint16_t>(std::stoul(stringValue));
            return ConstantUInt16ValueFunction(intValue);
        };
        case DataType::Type::UINT32: {
            const auto intValue = static_cast<uint32_t>(std::stoul(stringValue));
            return ConstantUInt32ValueFunction(intValue);
        };
        case DataType::Type::UINT64: {
            const auto intValue = static_cast<uint64_t>(std::stoull(stringValue));
            return ConstantUInt64ValueFunction(intValue);
        };
        case DataType::Type::INT8: {
            const auto intValue = static_cast<int8_t>(std::stoi(stringValue));
            return ConstantInt8ValueFunction(intValue);
        };
        case DataType::Type::INT16: {
            const auto intValue = static_cast<int16_t>(std::stoi(stringValue));
            return ConstantInt16ValueFunction(intValue);
        };
        case DataType::Type::INT32: {
            const auto intValue = static_cast<int32_t>(std::stoi(stringValue));
            return ConstantInt32ValueFunction(intValue);
        };
        case DataType::Type::INT64: {
            const auto intValue = static_cast<int64_t>(std::stol(stringValue));
            return ConstantInt64ValueFunction(intValue);
        };
        case DataType::Type::FLOAT32: {
            const auto floatValue = std::stof(stringValue);
            return ConstantFloatValueFunction(floatValue);
        };
        case DataType::Type::FLOAT64: {
            const auto doubleValue = std::stod(stringValue);
            return ConstantDoubleValueFunction(doubleValue);
        };
        case DataType::Type::BOOLEAN: {
            const auto boolValue = static_cast<int>(static_cast<bool>(std::stoi(stringValue))) == 1;
            return ConstantBooleanValueFunction(boolValue);
        };
        case DataType::Type::CHAR:
            break;
        case DataType::Type::VARSIZED_POINTER_REP:
        case DataType::Type::VARSIZED: {
            return ConstantValueVariableSizePhysicalFunction(reinterpret_cast<const int8_t*>(stringValue.c_str()), stringValue.size());
        };
        case DataType::Type::UNDEFINED: {
            throw UnknownPhysicalType("the UNKNOWN type is not supported");
        };
    }
    std::unreachable();
}
}
