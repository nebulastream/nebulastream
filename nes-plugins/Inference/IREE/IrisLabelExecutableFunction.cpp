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

#include <Execution/Functions/ExecutableFunctionConstantValueVariableSize.hpp>
#include <Functions/NodeFunction.hpp>
#include <ExecutableFunctionRegistry.hpp>

namespace NES::Runtime::Execution::Functions
{

struct IrisLabelExecutableFunction : Function
{
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override
    {
        auto setosaVal = setosa->execute(record, arena);
        auto versicolorVal = versicolor->execute(record, arena);
        auto virginicaVal = virginica->execute(record, arena);

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

    IrisLabelExecutableFunction(std::unique_ptr<Function> setosa, std::unique_ptr<Function> versicolor, std::unique_ptr<Function> virginica)
        : setosa(std::move(setosa)), versicolor(std::move(versicolor)), virginica(std::move(virginica))
    {
    }

    std::unique_ptr<Function> setosa;
    std::unique_ptr<Function> versicolor;
    std::unique_ptr<Function> virginica;

    ExecutableFunctionConstantValueVariableSize setosaConstantValue{"setosa"};
    ExecutableFunctionConstantValueVariableSize versicolorConstantValue{"versicolor"};
    ExecutableFunctionConstantValueVariableSize virginicaConstantValue{"virginica"};
};

}

namespace NES::Runtime::Execution::Functions::ExecutableFunctionGeneratedRegistrar
{
/// declaration of register functions for 'ExecutableFunctions'
ExecutableFunctionRegistryReturnType Registeriris_labelExecutableFunction(ExecutableFunctionRegistryArguments arguments)
{
    if (arguments.childFunctions.size() != 3)
    {
        throw TypeInferenceException("Function Expects 3 Arguments");
    }

    return std::make_unique<IrisLabelExecutableFunction>(
        std::move(arguments.childFunctions[0]), std::move(arguments.childFunctions[1]), std::move(arguments.childFunctions[2]));
}
}
