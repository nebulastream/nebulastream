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

#include <Functions/VarSizedToNumericPhysicalFunction.hpp>

#include <cctype>
#include <cstdint>
#include <string>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Strings.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <function.hpp>

namespace NES
{
namespace
{

std::string normalizeVarSizedNumericInput(const std::string& varSized)
{
    /// Remove all characters that are not digits, '.', '+', '-', or 'e'/'E' (for scientific notation)
    std::string inputCopy{varSized};
    std::erase_if(
        inputCopy,
        [](const char c)
        { return not std::isdigit(static_cast<unsigned char>(c)) && c != '.' && c != '+' && c != '-' && c != 'e' && c != 'E'; });

    const auto trimmed = NES::trimWhiteSpaces(inputCopy);
    if (trimmed.empty())
    {
        throw FormattingError("VarSizedToNumeric: cannot convert '{}' to a numeric value", varSized);
    }

    return std::string{trimmed};
}

template <typename NumericType>
VarVal invokeConvertAndWrap(const nautilus::val<uint64_t>& size, const nautilus::val<const char*>& ptr, bool nullable)
{
    const auto parsedValue = nautilus::invoke(
        +[](const uint64_t stringSize, const char* stringPtr) -> NumericType
        {
            const std::string varSizedSV{stringPtr, stringSize};
            const auto normalizedInput = normalizeVarSizedNumericInput(varSizedSV);
            try
            {
                return from_chars_with_exception<NumericType>(normalizedInput);
            }
            catch (const Exception&)
            {
                throw FormattingError("VarSizedToNumeric: cannot convert '{}' to target numeric type", normalizedInput);
            };
        },
        size,
        ptr);
    return VarVal{parsedValue, nullable, false};
}

}

VarVal VarSizedToNumericPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = child.execute(record, arena);

    if (value.isNullable() && value.isNull())
    {
        return VarVal{0, true, true}.castToType(outputType.type);
    }

    const auto var = value.getRawValueAs<VariableSizedData>();
    const auto size = var.getSize();
    const auto ptr = var.getContent();

    switch (outputType.type)
    {
        case DataType::Type::INT8:
            return invokeConvertAndWrap<int8_t>(size, ptr, outputType.nullable);
        case DataType::Type::INT16:
            return invokeConvertAndWrap<int16_t>(size, ptr, outputType.nullable);
        case DataType::Type::INT32:
            return invokeConvertAndWrap<int32_t>(size, ptr, outputType.nullable);
        case DataType::Type::INT64:
            return invokeConvertAndWrap<int64_t>(size, ptr, outputType.nullable);
        case DataType::Type::UINT8:
            return invokeConvertAndWrap<uint8_t>(size, ptr, outputType.nullable);
        case DataType::Type::UINT16:
            return invokeConvertAndWrap<uint16_t>(size, ptr, outputType.nullable);
        case DataType::Type::UINT32:
            return invokeConvertAndWrap<uint32_t>(size, ptr, outputType.nullable);
        case DataType::Type::UINT64:
            return invokeConvertAndWrap<uint64_t>(size, ptr, outputType.nullable);
        case DataType::Type::FLOAT32:
            return invokeConvertAndWrap<float>(size, ptr, outputType.nullable);
        case DataType::Type::FLOAT64:
            return invokeConvertAndWrap<double>(size, ptr, outputType.nullable);
        default:
            throw FormattingError("VarSizedToNumeric: unsupported target type {}", outputType);
    }
}

VarSizedToNumericPhysicalFunction::VarSizedToNumericPhysicalFunction(PhysicalFunction child, DataType outputType)
    : child(std::move(child)), outputType(std::move(outputType))
{
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterVarSizedToNumericPhysicalFunction(
    PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        physicalFunctionRegistryArguments.childFunctions.size() == 1, "VarSizedToNumeric function must have exactly one child function");

    return VarSizedToNumericPhysicalFunction(
        physicalFunctionRegistryArguments.childFunctions[0], physicalFunctionRegistryArguments.outputType);
}

}
