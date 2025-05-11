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
#include <Nautilus/DataTypes/VariableSizedData.hpp>

#include "PhysicalFunctionRegistry.hpp"

namespace NES
{

struct MaxValueExecutableFunction : PhysicalFunctionConcept
{
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override
    {
        auto input = child.execute(record, arena);
        auto varsized = input.cast<VariableSizedData>();

        nautilus::val<float> max = *static_cast<nautilus::val<float*>>(varsized.getContent());
        for (nautilus::val<size_t> i = 1; i < varsized.getContentSize() / sizeof(float); ++i)
        {
            nautilus::val<float> current = *static_cast<nautilus::val<float*>>(varsized.getContent() + i * sizeof(float));
            if (current > max)
            {
                max = current;
            }
        }

        return max;
    }

    MaxValueExecutableFunction(PhysicalFunction child) : child(std::move(child)) { }
    PhysicalFunction child;
};

}

namespace NES::PhysicalFunctionGeneratedRegistrar
{
/// declaration of register functions for 'ExecutableFunctions'
PhysicalFunctionRegistryReturnType RegisterMaxValuePhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    if (arguments.childFunctions.size() != 1)
    {
        throw TypeInferenceException("Function Expects 1 Argument");
    }

    return PhysicalFunction(MaxValueExecutableFunction(std::move(arguments.childFunctions[0])));
}
}
