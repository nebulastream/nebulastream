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

#include <Functions/CastFieldPhysicalFunction.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

/// TOINT64 executes as a plain field cast; the outputType is fixed to INT64 by the logical function.
PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterToInt64PhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1, "ToInt64 function must have exactly one child function");
    return CastFieldPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0], physicalFunctionRegistryArguments.outputType);
}

}
