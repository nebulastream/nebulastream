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

#include <IsNaNPhysicalFunction.hpp>

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Util/Strings.hpp>
#include <nautilus/std/cstring.h>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <function.hpp>
#include <val.hpp>

namespace NES
{

IsNaNPhysicalFunction::IsNaNPhysicalFunction(PhysicalFunction child) : child(std::move(child))
{
}

VarVal IsNaNPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    /// NaN is the only value that compares unequal to itself, so this needs no NaN constant
    /// and works for both quiet and signalling NaN.
    const auto leftValue = child.execute(record, arena).getRawValueAs<nautilus::val<double>>();
    return leftValue != leftValue;
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterIsNaNPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1, "IsNaN function must have exactly one child function");
    return IsNaNPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
}
}
