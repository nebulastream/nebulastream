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
#include <Functions/PhysicalFunction.hpp>
#include <Functions/LogicalFunctions/NegatePhysicalFunction.hpp>
#include <ExecutionContext.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES::Functions
{

VarVal NegatePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childFunction->execute(record, arena);
    return !value;
}

NegatePhysicalFunction::NegatePhysicalFunction(std::unique_ptr<PhysicalFunction> childFunction) : childFunction(std::move(childFunction))
{
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterNegatePhysicalFunction(PhysicalFunctionRegistryArguments PhysicalFunctionRegistryArguments)
{
    PRECONDITION(PhysicalFunctionRegistryArguments.childFunctions.size() == 1, "Negate function must have exactly one sub-function");
    return std::make_unique<NegatePhysicalFunction>(std::move(PhysicalFunctionRegistryArguments.childFunctions[0]));
}

}
