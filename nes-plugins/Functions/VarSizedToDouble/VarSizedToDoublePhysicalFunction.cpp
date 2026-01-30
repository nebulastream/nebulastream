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

#include "VarSizedToDoublePhysicalFunction.hpp"

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
#include <nautilus/std/cstring.h>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <function.hpp>
#include <val.hpp>

namespace NES
{

VarSizedToDoublePhysicalFunction::VarSizedToDoublePhysicalFunction(PhysicalFunction child) : child(std::move(child))
{
}

double convertVarSizedToDouble(const char* varSizedPtr, const uint32_t varSizedLen)
{
    /// Remove all occurrences of whitespaces ' ' and question marks '?' and try to parse the string as a double again
    std::string_view varSizedSV{varSizedPtr, varSizedLen};
    auto inputCopy = std::string{varSizedSV};
    inputCopy.erase(std::ranges::remove(inputCopy, '?').begin(), inputCopy.end());
    inputCopy.erase(std::ranges::remove(inputCopy, ' ').begin(), inputCopy.end());
    const auto splitSV = splitWithStringDelimiter<std::string_view>(varSizedSV, "?");
    try
    {
        const auto doubleVal = from_chars_with_exception<double>(inputCopy);
        return doubleVal;
    }
    catch (Exception)
    {
        NES_WARNING("Cannot convert string {} to double, returning NaN", varSizedSV);
        return std::numeric_limits<double>::quiet_NaN();
    }
}

VarVal VarSizedToDoublePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = child.execute(record, arena).cast<VariableSizedData>();
    const nautilus::val<double> doubleVal = nautilus::invoke(convertVarSizedToDouble, leftValue.getContent(), leftValue.getSize());
    return doubleVal;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterVarSizedToDoublePhysicalFunction(
    PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        physicalFunctionRegistryArguments.childFunctions.size() == 1, "VarSizedToDouble function must have exactly one child function");
    return VarSizedToDoublePhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
}
}
