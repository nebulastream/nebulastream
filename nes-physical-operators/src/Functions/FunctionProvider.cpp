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
#include <Functions/CastFieldPhysicalFunction.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/ConstantValuePhysicalFunction.hpp>
#include <Functions/ConstantValueVariableSizePhysicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Util/Strings.hpp>
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

namespace
{
template <typename T>
requires requires(std::string_view input) { from_chars<T>(input); } /// TODO #1035: check if two Util namespaces are needed
T parseConstantValue(std::string_view input)
{
    if (auto value = from_chars<T>(input))
    {
        return *value;
    }
    throw QueryCompilerError("Can not parse constant value \"{}\" into {}", input, typeid(T).name());
}
}

PhysicalFunction FunctionProvider::lowerConstantFunction(const ConstantValueLogicalFunction& constantFunction)
{
    const auto stringValue = constantFunction.getConstantValue();
    switch (constantFunction.getDataType().type)
    {
        case DataType::Type::UINT8:
            return ConstantUInt8ValueFunction(parseConstantValue<int8_t>(stringValue));
        case DataType::Type::UINT16:
            return ConstantUInt16ValueFunction(parseConstantValue<int16_t>(stringValue));
        case DataType::Type::UINT32:
            return ConstantUInt32ValueFunction(parseConstantValue<uint32_t>(stringValue));
        case DataType::Type::UINT64:
            return ConstantUInt64ValueFunction(parseConstantValue<uint64_t>(stringValue));
        case DataType::Type::INT8:
            return ConstantInt8ValueFunction(parseConstantValue<int8_t>(stringValue));
        case DataType::Type::INT16:
            return ConstantInt16ValueFunction(parseConstantValue<int16_t>(stringValue));
        case DataType::Type::INT32:
            return ConstantInt32ValueFunction(parseConstantValue<int32_t>(stringValue));
        case DataType::Type::INT64:
            return ConstantInt64ValueFunction(parseConstantValue<int64_t>(stringValue));
        case DataType::Type::FLOAT32:
            return ConstantFloatValueFunction(parseConstantValue<float>(stringValue));
        case DataType::Type::FLOAT64:
            return ConstantDoubleValueFunction(parseConstantValue<double>(stringValue));
        case DataType::Type::BOOLEAN:
            return ConstantBooleanValueFunction(parseConstantValue<bool>(stringValue));
        case DataType::Type::CHAR:
            return ConstantCharValueFunction(parseConstantValue<char>(stringValue));
        case DataType::Type::VARSIZED_POINTER_REP:
        case DataType::Type::VARSIZED: {
            return ConstantValueVariableSizePhysicalFunction(std::bit_cast<const int8_t*>(stringValue.c_str()), stringValue.size());
        };
        case DataType::Type::UNDEFINED: {
            throw UnknownPhysicalType("the UNKNOWN type is not supported");
        };
    }
    std::unreachable();
}
}
