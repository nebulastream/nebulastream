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

#include <Functions/ArithmeticalFunctions/PowPhysicalFunction.hpp>

#include <utility>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <std/cmath.h>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val.hpp>

namespace NES
{
PowPhysicalFunction::PowPhysicalFunction(PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction, DataType outputType)
    : leftPhysicalFunction(std::move(leftPhysicalFunction))
    , rightPhysicalFunction(std::move(rightPhysicalFunction))
    , outputType(std::move(outputType))
{
}

VarVal PowPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = leftPhysicalFunction.execute(record, arena);
    const auto rightValue = rightPhysicalFunction.execute(record, arena);
    return VarVal{nautilus::pow(leftValue.cast<nautilus::val<double>>(), rightValue.cast<nautilus::val<double>>())}.castToType(
        outputType.type);
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterPowPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 2, "Pow function must have exactly two child functions");
    return {
        PowPhysicalFunction(
            physicalFunctionRegistryArguments.childFunctions[0],
            physicalFunctionRegistryArguments.childFunctions[1],
            physicalFunctionRegistryArguments.outputType),
        physicalFunctionRegistryArguments.childFunctions};
}
}
