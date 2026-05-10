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

#include <ReadingGreaterPhysicalFunction.hpp>

#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/val.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

ReadingGreaterPhysicalFunction::ReadingGreaterPhysicalFunction(PhysicalFunction leftFunction, PhysicalFunction rightFunction)
    : leftFunction(std::move(leftFunction)), rightFunction(std::move(rightFunction))
{
}

VarVal ReadingGreaterPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto left = leftFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto right = rightFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto leftTemp = left.at("temperature").getRawValueAs<nautilus::val<double>>();
    const auto rightTemp = right.at("temperature").getRawValueAs<nautilus::val<double>>();
    return VarVal{leftTemp > rightTemp};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterGreater_Reading_ReadingPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2, "Greater_Reading_Reading expects exactly two child functions");
    return ReadingGreaterPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1]);
}

}
