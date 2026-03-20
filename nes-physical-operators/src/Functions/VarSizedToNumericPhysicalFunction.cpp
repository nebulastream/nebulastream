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

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <Arena.hpp>
#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <Util/Strings.hpp>
#include <function.hpp>

namespace NES
    {
        namespace
        {

        std::string normalizeVarSizedNumericInput(const char* varSizedPtr, const uint32_t varSizedLen)
        {
            std::string_view varSizedSV{varSizedPtr, varSizedLen};
            auto inputCopy = std::string{varSizedSV};

            /// Remove all occurrences of '?' and spaces ' '
            inputCopy.erase(std::ranges::remove(inputCopy, '?').begin(), inputCopy.end());
            inputCopy.erase(std::ranges::remove(inputCopy, ' ').begin(), inputCopy.end());

            const auto trimmed = NES::trimWhiteSpaces(inputCopy);
            if (trimmed.empty())
            {
                throw FormattingError("VarSizedToNumeric: cannot convert '{}' to a numeric value", varSizedSV);
            }

            return std::string{trimmed};
        }

        template<typename NumericType>
        NumericType parseNormalizedNumeric(std::string_view normalizedInput)
        {
            try
            {
                return from_chars_with_exception<NumericType>(normalizedInput);
            }
            catch (const Exception&)
            {
                throw FormattingError("VarSizedToNumeric: cannot convert '{}' to target numeric type", normalizedInput);
            }
        }

        template<typename NumericType>
        NumericType convertVarSizedToNumeric(const char* varSizedPtr, const uint32_t varSizedLen)
        {
            const auto normalizedInput = normalizeVarSizedNumericInput(varSizedPtr, varSizedLen);
            return parseNormalizedNumeric<NumericType>(normalizedInput);
        }

        }

        VarSizedToNumericPhysicalFunction::VarSizedToNumericPhysicalFunction(PhysicalFunction child, DataType outputType)
            : child(std::move(child)), outputType(std::move(outputType))
        {
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
                {
                    const auto parsedValue = nautilus::invoke(
                        +[](const uint32_t stringSize, const char* stringPtr) -> int8_t
                        {
                            return convertVarSizedToNumeric<int8_t>(stringPtr, stringSize);
                        },
                        size,
                        ptr);
                    return VarVal{parsedValue, false, false};
                }
                case DataType::Type::INT16:
                {
                    const auto parsedValue = nautilus::invoke(
                        +[](const uint32_t stringSize, const char* stringPtr) -> int16_t
                        {
                            return convertVarSizedToNumeric<int16_t>(stringPtr, stringSize);
                        },
                        size,
                        ptr);
                    return VarVal{parsedValue, false, false};
                }
                case DataType::Type::INT32:
                {
                    const auto parsedValue = nautilus::invoke(
                        +[](const uint32_t stringSize, const char* stringPtr) -> int32_t
                        {
                            return convertVarSizedToNumeric<int32_t>(stringPtr, stringSize);
                        },
                        size,
                        ptr);
                    return VarVal{parsedValue, false, false};
                }
                case DataType::Type::INT64:
                {
                    const auto parsedValue = nautilus::invoke(
                        +[](const uint32_t stringSize, const char* stringPtr) -> int64_t
                        {
                            return convertVarSizedToNumeric<int64_t>(stringPtr, stringSize);
                        },
                        size,
                        ptr);
                    return VarVal{parsedValue, false, false};
                }
                case DataType::Type::UINT8:
                {
                    const auto parsedValue = nautilus::invoke(
                        +[](const uint32_t stringSize, const char* stringPtr) -> uint8_t
                        {
                            return convertVarSizedToNumeric<uint8_t>(stringPtr, stringSize);
                        },
                        size,
                        ptr);
                    return VarVal{parsedValue, false, false};
                }
                case DataType::Type::UINT16:
                {
                    const auto parsedValue = nautilus::invoke(
                        +[](const uint32_t stringSize, const char* stringPtr) -> uint16_t
                        {
                            return convertVarSizedToNumeric<uint16_t>(stringPtr, stringSize);
                        },
                        size,
                        ptr);
                    return VarVal{parsedValue, false, false};
                }
                case DataType::Type::UINT32:
                {
                    const auto parsedValue = nautilus::invoke(
                        +[](const uint32_t stringSize, const char* stringPtr) -> uint32_t
                        {
                            return convertVarSizedToNumeric<uint32_t>(stringPtr, stringSize);
                        },
                        size,
                        ptr);
                    return VarVal{parsedValue, false, false};
                }
                case DataType::Type::UINT64:
                {
                    const auto parsedValue = nautilus::invoke(
                        +[](const uint32_t stringSize, const char* stringPtr) -> uint64_t
                        {
                            return convertVarSizedToNumeric<uint64_t>(stringPtr, stringSize);
                        },
                        size,
                        ptr);
                    return VarVal{parsedValue, false, false};
                }
                case DataType::Type::FLOAT32:
                {
                    const auto parsedValue = nautilus::invoke(
                        +[](const uint32_t stringSize, const char* stringPtr) -> float
                        {
                            return convertVarSizedToNumeric<float>(stringPtr, stringSize);
                        },
                        size,
                        ptr);
                    return VarVal{parsedValue, false, false};
                }
                case DataType::Type::FLOAT64:
                {
                    const auto parsedValue = nautilus::invoke(
                        +[](const uint32_t stringSize, const char* stringPtr) -> double
                        {
                            return convertVarSizedToNumeric<double>(stringPtr, stringSize);
                        },
                        size,
                        ptr);
                    return VarVal{parsedValue, false, false};
                }
                default:
                    throw FormattingError("VarSizedToNumeric: unsupported target type {}", outputType);
            }
        }

        PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterVarSizedToNumericPhysicalFunction(
            PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
        {
            PRECONDITION(
                physicalFunctionRegistryArguments.childFunctions.size() == 1,
                "VarSizedToNumeric function must have exactly one child function");

            return VarSizedToNumericPhysicalFunction(
                physicalFunctionRegistryArguments.childFunctions[0],
                physicalFunctionRegistryArguments.outputType);
        }

    }