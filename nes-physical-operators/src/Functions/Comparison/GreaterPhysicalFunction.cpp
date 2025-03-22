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

#include <memory>
#include <utility>
#include <vector>
#include <Functions/Comparison/GreaterPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>


namespace NES::Functions
{

VarVal GreaterPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = leftPhysicalFunction.execute(record, arena);
    const auto rightValue = rightPhysicalFunction.execute(record, arena);
    return leftValue > rightValue;
}

GreaterPhysicalFunction::GreaterPhysicalFunction(
    PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction)
    : leftPhysicalFunction(leftPhysicalFunction), rightPhysicalFunction(rightPhysicalFunction)
{
}
PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterGreaterPhysicalFunction(
    PhysicalFunctionRegistryArguments PhysicalFunctionRegistryArguments)
{
    PRECONDITION(PhysicalFunctionRegistryArguments.childFunctions.size() == 2, "Greater function must have exactly two sub-functions");
    return GreaterPhysicalFunction(
        PhysicalFunctionRegistryArguments.childFunctions[0], PhysicalFunctionRegistryArguments.childFunctions[1]);
}

}
