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
#include <Functions/ConstantValuePhysicalFunction.hpp>
#include <Functions/ConstantValueVariableSizePhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES::QueryCompilation
{
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

PhysicalFunction FunctionProvider::lowerConstantFunction(const DataType& type, const std::string& stringValue)
{
    switch (type.type)
    {
        case DataType::Type::UINT8:
            return ConstantUInt8ValueFunction(parseConstantValue<uint8_t>(stringValue));
        case DataType::Type::UINT16:
            return ConstantUInt16ValueFunction(parseConstantValue<uint16_t>(stringValue));
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
            return ConstantValueVariableSizePhysicalFunction(reinterpret_cast<const int8_t*>(stringValue.c_str()), stringValue.size());
        };
        case DataType::Type::UNDEFINED: {
            throw UnknownPhysicalType("the UNKNOWN type is not supported");
        };
    }
    std::unreachable();
}
}
