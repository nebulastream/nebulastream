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

#include <Functions/CompressionPhysicalFunction.hpp>

#include <utility>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

VarVal CompressionPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    (void) record;
    (void) arena;
    return VarVal(nautilus::val<int>(32));
}

CompressionPhysicalFunction::CompressionPhysicalFunction()
{
}

// PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterCompressionPhysicalFunction(
//     PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
// {
//     return CompressionPhysicalFunction();
// }

}
