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

#include <Functions/OctetLengthPhysicalFunction.hpp>

#include <utility>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

OctetLengthPhysicalFunction::OctetLengthPhysicalFunction(PhysicalFunction childPhysicalFunction)
    : childPhysicalFunction(std::move(childPhysicalFunction))
{
}

VarVal OctetLengthPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto inputValue = childPhysicalFunction.execute(record, arena);
    const auto variableSizedData = inputValue.getRawValueAs<VariableSizedData>();
    return VarVal{variableSizedData.getSize(), inputValue.isNullable(), inputValue.isNull()};
}

PhysicalFunctionRegistryReturnType
OctetLengthPhysicalFunction::createOCTET_LENGTH(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        physicalFunctionRegistryArguments.childFunctions.size() == 1, "OCTET_LENGTH function must have exactly one child function");
    return OctetLengthPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
}
}
