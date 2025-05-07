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
#include <ranges>

#include <Execution/Functions/ExecutableFunctionConstantValueVariableSize.hpp>
#include <Functions/NodeFunction.hpp>
#include <boost/concept_archetype.hpp>
#include <ExecutableFunctionRegistry.hpp>

namespace NES::Runtime::Execution::Functions
{
static constexpr auto Labels = std::to_array<std::string_view>(
    {"_background_noise_",
     "backward",
     "bed",
     "bird",
     "cat",
     "dog",
     "down",
     "eight",
     "five",
     "follow",
     "forward",
     "four",
     "go",
     "happy",
     "house",
     "learn",
     "left",
     "marvin",
     "nine",
     "no",
     "off",
     "on",
     "one",
     "right",
     "seven",
     "sheila",
     "six",
     "stop",
     "three",
     "tree",
     "two",
     "up",
     "visual",
     "wow",
     "yes",
     "zero"});

template <uint64_t index>
const ExecutableFunctionConstantValueVariableSize& switchStatement(
    nautilus::val<uint64_t>& argmax,
    const std::vector<ExecutableFunctionConstantValueVariableSize>& labels,
    const ExecutableFunctionConstantValueVariableSize& invalid)
{
    if (argmax == nautilus::val<uint64_t>(index - 1))
    {
        return labels[index - 1];
    }
    [[clang::musttail]] return switchStatement<index - 1>(argmax, labels, invalid);
}

template <>
const ExecutableFunctionConstantValueVariableSize& switchStatement<0>(
    nautilus::val<uint64_t>&,
    const std::vector<ExecutableFunctionConstantValueVariableSize>&,
    const ExecutableFunctionConstantValueVariableSize& invalid)
{
    return invalid;
}

struct SpeechRecognitionLabelExecutableFunction : Function
{
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override
    {
        auto index = argmax->execute(record, arena).cast<nautilus::val<uint64_t>>();
        return switchStatement<Labels.size()>(index, labels, invalid).execute(record, arena);
    }

    SpeechRecognitionLabelExecutableFunction(std::unique_ptr<Function> argmax)
        : argmax(std::move(argmax))
        , labels(
              std::views::transform(Labels, [](auto&& label) { return ExecutableFunctionConstantValueVariableSize{label}; })
              | std::ranges::to<std::vector>())
    {
    }

    std::unique_ptr<Function> argmax;
    std::vector<ExecutableFunctionConstantValueVariableSize> labels;

    ExecutableFunctionConstantValueVariableSize invalid{"INVALID"};
};

}

namespace NES::Runtime::Execution::Functions::ExecutableFunctionGeneratedRegistrar
{
/// declaration of register functions for 'ExecutableFunctions'
ExecutableFunctionRegistryReturnType RegisterSpeechRecognitionLabelExecutableFunction(ExecutableFunctionRegistryArguments arguments)
{
    if (arguments.childFunctions.size() != 1)
    {
        throw TypeInferenceException("Function Expects 1 Arguments");
    }

    return std::unique_ptr<SpeechRecognitionLabelExecutableFunction>(
        new SpeechRecognitionLabelExecutableFunction(std::move(arguments.childFunctions[0])));
}
}
