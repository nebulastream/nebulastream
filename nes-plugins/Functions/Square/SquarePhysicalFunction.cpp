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

// Generated from: square.extension.md

#include <SquarePhysicalFunction.hpp>

#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

SquarePhysicalFunction::SquarePhysicalFunction(PhysicalFunction childFunction, DataType inputType, DataType outputType)
    : childFunction(std::move(childFunction)), inputType(std::move(inputType)), outputType(std::move(outputType))
{
}

VarVal SquarePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto value = childFunction.execute(record, arena);
    return (value * value).castToType(outputType.type);
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterSquarePhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1, "Square function must have exactly one child function");
    PRECONDITION(physicalFunctionRegistryArguments.inputTypes.size() == 1, "Square function must have exactly one input type");
    return SquarePhysicalFunction(
        physicalFunctionRegistryArguments.childFunctions[0],
        physicalFunctionRegistryArguments.inputTypes[0],
        physicalFunctionRegistryArguments.outputType);
}

}
