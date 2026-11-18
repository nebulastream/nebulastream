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

#include <Functions/NullValuePhysicalFunction.hpp>

#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <NullValue.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

NullValuePhysicalFunction::NullValuePhysicalFunction(DataType dataType) : dataType(std::move(dataType))
{
}

VarVal NullValuePhysicalFunction::execute(const Record&, ArenaRef&) const
{
    return makeNullVarVal(dataType);
}

PhysicalFunctionRegistryReturnType NullValuePhysicalFunction::createNullValue(PhysicalFunctionRegistryArguments arguments)
{
    return NullValuePhysicalFunction(arguments.outputType);
}

}
