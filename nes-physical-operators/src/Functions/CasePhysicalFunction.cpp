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
#include <Functions/CasePhysicalFunction.hpp>

#include <utility>
#include <vector>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <select.hpp>
#include <val_bool.hpp>

namespace NES
{

VarVal CasePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    VarVal result = defaultResult.execute(record, arena);
    for (int i = (int)whenConditions.size() - 1; i >= 0; --i) {
        auto cond = whenConditions[i].execute(record, arena);
        auto res = thenResults[i].execute(record, arena);
        
        result = VarVal::select(cond.getRawValueAs<nautilus::val<bool>>(), res, result);
    }
    return result;
}

CasePhysicalFunction::CasePhysicalFunction(std::vector<PhysicalFunction> whenConditions, std::vector<PhysicalFunction> thenResults, PhysicalFunction defaultResult)
    : whenConditions(std::move(whenConditions)), thenResults(std::move(thenResults)), defaultResult(std::move(defaultResult))
{
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterCasePhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    if (arguments.childFunctions.size() < 3 || arguments.childFunctions.size() % 2 != 1)
    {
        throw CannotDeserialize("CasePhysicalFunction requires an odd number of child functions >= 3");
    }
    std::vector<PhysicalFunction> whens;
    std::vector<PhysicalFunction> thens;
    size_t pairCount = (arguments.childFunctions.size() - 1) / 2;
    for (size_t i = 0; i < pairCount; ++i) {
        whens.push_back(arguments.childFunctions[2 * i]);
        thens.push_back(arguments.childFunctions[2 * i + 1]);
    }
    return CasePhysicalFunction(std::move(whens), std::move(thens), arguments.childFunctions.back());
}

}
