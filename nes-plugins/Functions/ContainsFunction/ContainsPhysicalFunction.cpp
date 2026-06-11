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

#include <ContainsPhysicalFunction.hpp>

#include <cstdint>
#include <string_view>
#include <utility>

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val.hpp>

namespace NES
{

ContainsPhysicalFunction::ContainsPhysicalFunction(PhysicalFunction haystackPhysicalFunction, PhysicalFunction needlePhysicalFunction)
    : haystackPhysicalFunction(std::move(haystackPhysicalFunction)), needlePhysicalFunction(std::move(needlePhysicalFunction))
{
}

namespace
{
bool containsString(const int8_t* haystackPtr, uint64_t haystackSize, const int8_t* needlePtr, uint64_t needleSize)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    const std::string_view haystack(reinterpret_cast<const char*>(haystackPtr), haystackSize);
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    const std::string_view needle(reinterpret_cast<const char*>(needlePtr), needleSize);
    return haystack.contains(needle);
}
}

VarVal ContainsPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto haystackValue = haystackPhysicalFunction.execute(record, arena);
    const auto needleValue = needlePhysicalFunction.execute(record, arena);

    const bool nullable = haystackValue.isNullable() or needleValue.isNullable();
    const auto isNull = nautilus::val<bool>(nullable) and (haystackValue.isNull() or needleValue.isNull());

    /// Mirrors ConcatPhysicalFunction: if any input is NULL, skip the native call — getContent()
    /// on a NULL VARSIZED returns a pointer we must not dereference.
    if (isNull)
    {
        return VarVal{nautilus::val<bool>(false), nullable, isNull};
    }

    const auto haystack = haystackValue.getRawValueAs<VariableSizedData>();
    const auto needle = needleValue.getRawValueAs<VariableSizedData>();
    const auto result = nautilus::invoke(containsString, haystack.getContent(), haystack.getSize(), needle.getContent(), needle.getSize());
    return VarVal{result, nullable, isNull};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterContainsPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 2, "Contains function must have exactly two child functions");
    return ContainsPhysicalFunction(
        physicalFunctionRegistryArguments.childFunctions[0], physicalFunctionRegistryArguments.childFunctions[1]);
}

}
