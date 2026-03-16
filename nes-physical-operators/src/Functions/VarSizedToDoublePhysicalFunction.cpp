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

#include <Functions/VarSizedToDoublePhysicalFunction.hpp>

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

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

        double convertVarSizedToDouble(const char* varSizedPtr, const uint32_t varSizedLen)
        {
            std::string_view varSizedSV{varSizedPtr, varSizedLen};
            auto inputCopy = std::string{varSizedSV};

            /// Remove all occurrences of '?' and spaces ' '
            inputCopy.erase(std::ranges::remove(inputCopy, '?').begin(), inputCopy.end());
            inputCopy.erase(std::ranges::remove(inputCopy, ' ').begin(), inputCopy.end());

            const auto trimmed = NES::trimWhiteSpaces(inputCopy);
            if (trimmed.empty())
            {
                throw FormattingError("VarSizedToDouble: cannot convert '{}' to double", varSizedSV);
            }

            try
            {
                return from_chars_with_exception<double>(trimmed);
            }
            catch (const Exception&)
            {
                throw FormattingError("VarSizedToDouble: cannot convert '{}' to double", varSizedSV);
            }
        }

        }

        VarSizedToDoublePhysicalFunction::VarSizedToDoublePhysicalFunction(PhysicalFunction child) : child(std::move(child))
        {
        }

        VarVal VarSizedToDoublePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
        {
            const auto value = child.execute(record, arena);
            const auto var = value.getRawValueAs<VariableSizedData>();
            const auto size = var.getSize();
            const auto ptr = var.getContent();

            const auto parsedDouble = nautilus::invoke(
                +[](const uint32_t stringSize, const char* stringPtr) -> double
                {
                    return convertVarSizedToDouble(stringPtr, stringSize);
                },
                size,
                ptr);

            return VarVal{parsedDouble, false, false};
        }

        PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterVarSizedToDoublePhysicalFunction(
            PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
        {
            PRECONDITION(
                physicalFunctionRegistryArguments.childFunctions.size() == 1,
                "VarSizedToDouble function must have exactly one child function");
            return VarSizedToDoublePhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
        }

    }