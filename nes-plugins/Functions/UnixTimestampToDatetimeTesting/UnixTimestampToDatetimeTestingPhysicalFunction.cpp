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

#include "UnixTimestampToDatetimeTestingPhysicalFunction.hpp"

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

UnixTimestampToDatetimeTestingPhysicalFunction::UnixTimestampToDatetimeTestingPhysicalFunction(PhysicalFunction child) : child(std::move(child))
{
}


PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterUnixTimestampToDatetimeTestingPhysicalFunction(
    PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        physicalFunctionRegistryArguments.childFunctions.size() == 1, "UnixTimestampToDatetimeTesting function must have exactly one child function");
    return UnixTimestampToDatetimeTestingPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
}
}