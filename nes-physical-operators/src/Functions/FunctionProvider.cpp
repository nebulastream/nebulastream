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
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/ConstantValuePhysicalFunction.hpp>
#include <Functions/ConstantValueVariableSizePhysicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Schema/Binder.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES::QueryCompilation
{
PhysicalFunction FunctionProvider::lowerFunction(LogicalFunction logicalFunction, const FieldMappingTrait& fieldMappingTrait)
{
    /// 1. Recursively lower the children of the function node.
    std::vector<PhysicalFunction> childFunctions;
    std::vector<DataType> inputTypes;
    for (const auto& child : logicalFunction.getChildren())
    {
        childFunctions.emplace_back(lowerFunction(child, fieldMappingTrait));
        inputTypes.emplace_back(child.getDataType());
    }

    /// 2. The field access and constant value nodes are special as they require a different treatment,
    /// due to them not simply getting a childFunction as a parameter.
    if (const auto fieldAccessFunction = logicalFunction.tryGetAs<FieldAccessLogicalFunction>())
    {
        const auto mappedName = fieldMappingTrait.getMapping(unbind(fieldAccessFunction.value()->getField()));
        INVARIANT(mappedName.has_value(), "Can not find mapping for {}", fieldAccessFunction.value()->getField());
        return FieldAccessPhysicalFunction(mappedName.value());
    }
    if (const auto constantValueFunction = logicalFunction.tryGetAs<ConstantValueLogicalFunction>())
    {
        return lowerConstantFunction(constantValueFunction->get());
    }

    /// 3. Calling the registry to create an executable function.
    if (const auto factory = PhysicalFunctionRegistry::instance().find(std::string(logicalFunction.getType())))
    {
        return (*factory)(PhysicalFunctionRegistryArguments{
            .childFunctions = childFunctions, .inputTypes = inputTypes, .outputType = logicalFunction.getDataType()});
    }
    throw UnknownFunctionType("Can not lower function: {}", logicalFunction);
}

PhysicalFunction FunctionProvider::lowerConstantFunction(const ConstantValueLogicalFunction& constantFunction)
{
    const auto& nesVal = constantFunction.getValue();
    const bool nullable = constantFunction.getDataType().nullable;
    const bool isNull = constantFunction.isNullValue();
    const auto lowerFixed = [&]<typename FnType, typename ValueType>() -> PhysicalFunction
    {
        if (isNull)
        {
            return FnType(ValueType{}, /*nullable=*/true, /*isNull=*/true);
        }
        const auto* typedPtr = nesVal.tryGetAs<ValueType>();
        if (typedPtr == nullptr)
        {
            throw QueryCompilerError(
                "ConstantValueLogicalFunction NESValue payload does not match expected physical type {}", NAMEOF_TYPE(ValueType));
        }
        return FnType(*typedPtr, nullable, /*isNull=*/false);
    };
    switch (constantFunction.getDataType().type)
    {
        case DataType::Type::UINT8:
            return lowerFixed.template operator()<ConstantUInt8ValueFunction, uint8_t>();
        case DataType::Type::UINT16:
            return lowerFixed.template operator()<ConstantUInt16ValueFunction, uint16_t>();
        case DataType::Type::UINT32:
            return lowerFixed.template operator()<ConstantUInt32ValueFunction, uint32_t>();
        case DataType::Type::UINT64:
            return lowerFixed.template operator()<ConstantUInt64ValueFunction, uint64_t>();
        case DataType::Type::INT8:
            return lowerFixed.template operator()<ConstantInt8ValueFunction, int8_t>();
        case DataType::Type::INT16:
            return lowerFixed.template operator()<ConstantInt16ValueFunction, int16_t>();
        case DataType::Type::INT32:
            return lowerFixed.template operator()<ConstantInt32ValueFunction, int32_t>();
        case DataType::Type::INT64:
            return lowerFixed.template operator()<ConstantInt64ValueFunction, int64_t>();
        case DataType::Type::FLOAT32:
            return lowerFixed.template operator()<ConstantFloatValueFunction, float>();
        case DataType::Type::FLOAT64:
            return lowerFixed.template operator()<ConstantDoubleValueFunction, double>();
        case DataType::Type::BOOLEAN:
            return lowerFixed.template operator()<ConstantBooleanValueFunction, bool>();
        case DataType::Type::CHAR:
            return lowerFixed.template operator()<ConstantCharValueFunction, char>();
        case DataType::Type::VARSIZED: {
            if (isNull)
            {
                return ConstantValueVariableSizePhysicalFunction(nullptr, 0, /*nullable=*/true, /*isNull=*/true);
            }
            const auto* strPtr = nesVal.tryGetAs<std::string>();
            if (strPtr == nullptr)
            {
                throw QueryCompilerError("ConstantValueLogicalFunction NESValue payload does not match VARSIZED std::string");
            }
            return ConstantValueVariableSizePhysicalFunction(
                reinterpret_cast<const int8_t*>(strPtr->c_str()), strPtr->size(), nullable, /*isNull=*/false);
        };
        case DataType::Type::UNDEFINED: {
            throw UnknownPhysicalType("the UNKNOWN type is not supported");
        };
    }
    std::unreachable();
}
}
