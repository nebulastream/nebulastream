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

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
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
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES::QueryCompilation
{
PhysicalFunction FunctionProvider::lowerFunction(LogicalFunction logicalFunction)
{
    /// 1. Recursively lower the children of the function node.
    std::vector<PhysicalFunction> childFunctions;
    std::vector<DataType> inputTypes;
    for (const auto& child : logicalFunction.getChildren())
    {
        childFunctions.emplace_back(lowerFunction(child));
        inputTypes.emplace_back(child.getDataType());
    }

    /// 2. The field access and constant value nodes are special as they require a different treatment,
    /// due to them not simply getting a childFunction as a parameter.
    if (const auto fieldAccessFunction = logicalFunction.tryGetAs<FieldAccessLogicalFunction>())
    {
        return FieldAccessPhysicalFunction(fieldAccessFunction->get().getFieldName());
    }
    if (const auto constantValueFunction = logicalFunction.tryGetAs<ConstantValueLogicalFunction>())
    {
        return lowerConstantFunction(constantValueFunction->get());
    }

    /// 3. Calling the registry to create an executable function.
    PhysicalFunctionRegistryArguments executableFunctionArguments{
        .childFunctions = childFunctions, .inputTypes = inputTypes, .outputType = logicalFunction.getDataType()};

    /// 3a. Per-struct operator dispatch. When at least one operand is a STRUCT, prefer a
    /// keyed plugin variant `<FuncType>_<typeKey(L)>_<typeKey(R)>` so plugins can register
    /// struct-specific semantics (e.g. `Greater_Reading_Reading`) without core knowing about
    /// any particular struct. Falls through to the generic registry if no override exists.
    const auto baseName = std::string(logicalFunction.getType());
    if (inputTypes.size() == 2 && std::ranges::any_of(inputTypes, [](const auto& t) { return t.type == DataType::Type::STRUCT; }))
    {
        const auto typeKey = [](const DataType& dt) -> std::string
        {
            return dt.type == DataType::Type::STRUCT ? dt.structName : std::string(magic_enum::enum_name(dt.type));
        };
        const auto specializedName = fmt::format("{}_{}_{}", baseName, typeKey(inputTypes[0]), typeKey(inputTypes[1]));
        if (const auto function = PhysicalFunctionRegistry::instance().create(specializedName, executableFunctionArguments))
        {
            return function.value();
        }
    }

    if (const auto function = PhysicalFunctionRegistry::instance().create(baseName, std::move(executableFunctionArguments)))
    {
        return function.value();
    }
    throw UnknownFunctionType("Can not lower function: {}", logicalFunction);
}

namespace
{
template <typename T>
requires requires(std::string_view input) { from_chars<T>(input); }
T parseConstantValue(std::string_view input)
{
    if (auto value = from_chars<T>(input))
    {
        return *value;
    }
    throw QueryCompilerError("Can not parse constant value \"{}\" into {}", input, NAMEOF_TYPE(T));
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
        case DataType::Type::VARSIZED: {
            return ConstantValueVariableSizePhysicalFunction(std::bit_cast<const int8_t*>(stringValue.c_str()), stringValue.size());
        };
        case DataType::Type::FIXEDSIZED: {
            throw UnknownPhysicalType("FIXEDSIZED arrays cannot appear as constant values");
        };
        case DataType::Type::VARARRAY: {
            throw UnknownPhysicalType("VARARRAY types cannot appear as constant values (PoC).");
        };
        case DataType::Type::STRUCT: {
            throw UnknownPhysicalType("STRUCT types cannot appear as constant values (PoC).");
        };
        case DataType::Type::UNDEFINED: {
            throw UnknownPhysicalType("the UNKNOWN type is not supported");
        };
    }
    std::unreachable();
}
}
