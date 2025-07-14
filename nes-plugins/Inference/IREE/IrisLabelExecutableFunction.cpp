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
#include <Functions/ConstantValuePhysicalFunction.hpp>
#include <Functions/ConstantValueVariableSizePhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <folly/Function.h>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

struct IrisLabelPhysicalFunction final : PhysicalFunctionConcept
{
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override
    {
        auto setosaVal = setosa.execute(record, arena);
        auto versicolorVal = versicolor.execute(record, arena);
        auto virginicaVal = virginica.execute(record, arena);

        if (setosaVal >= versicolorVal && setosaVal >= virginicaVal)
        {
            return setosaConstantValue.execute(record, arena);
        }
        else if (versicolorVal >= virginicaVal)
        {
            return versicolorConstantValue.execute(record, arena);
        }
        else
        {
            return virginicaConstantValue.execute(record, arena);
        }
    }

    IrisLabelPhysicalFunction(PhysicalFunction setosa, PhysicalFunction versicolor, PhysicalFunction virginica)
        : setosa(std::move(setosa)), versicolor(std::move(versicolor)), virginica(std::move(virginica))
    {
    }

    PhysicalFunction setosa;
    PhysicalFunction versicolor;
    PhysicalFunction virginica;

    ConstantValueVariableSizePhysicalFunction setosaConstantValue{"setosa"};
    ConstantValueVariableSizePhysicalFunction versicolorConstantValue{"versicolor"};
    ConstantValueVariableSizePhysicalFunction virginicaConstantValue{"virginica"};
};

}

namespace NES::PhysicalFunctionGeneratedRegistrar
{
PhysicalFunctionRegistryReturnType Registeriris_labelPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    if (arguments.childFunctions.size() != 3)
    {
        throw TypeInferenceException("Function Expects 3 Arguments");
    }

    return PhysicalFunction(IrisLabelPhysicalFunction(
        std::move(arguments.childFunctions[0]), std::move(arguments.childFunctions[1]), std::move(arguments.childFunctions[2])));
}
}
