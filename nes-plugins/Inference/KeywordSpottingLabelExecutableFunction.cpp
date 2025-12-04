#include <memory>
#include <Execution/Functions/ExecutableFunctionConstantValueVariableSize.hpp>
#include <Functions/NodeFunction.hpp>
#include <ExecutableFunctionRegistry.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>

namespace NES::Runtime::Execution::Functions {

static constexpr auto Labels = std::to_array<std::string_view>({
    "one", "two", "three", "_unknown_"
});

template <uint64_t index>
const ExecutableFunctionConstantValueVariableSize& switchStatement(
    nautilus::val<uint64_t>& argmax,
    const std::vector<ExecutableFunctionConstantValueVariableSize>& labels,
    const ExecutableFunctionConstantValueVariableSize& invalid)
{
    if (argmax == nautilus::val<uint64_t>(index - 1)) {
        return labels[index - 1];
    }
    [[clang::musttail]] return switchStatement<index - 1>(argmax, labels, invalid);
}

template <>
inline const ExecutableFunctionConstantValueVariableSize& switchStatement<0>(
    nautilus::val<uint64_t>&,
    const std::vector<ExecutableFunctionConstantValueVariableSize>&,
    const ExecutableFunctionConstantValueVariableSize& invalid)
{
    return invalid;
}

struct KeywordSpottingLabelExecutableFunction : Function {
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override {
        auto index = argmax->execute(record, arena).cast<nautilus::val<uint64_t>>();
        return switchStatement<Labels.size()>(index, labels, invalid).execute(record, arena);
    }

    explicit KeywordSpottingLabelExecutableFunction(std::unique_ptr<Function> argmax)
        : argmax(std::move(argmax))
        , labels(std::views::transform(Labels, [](auto&& l){
            return ExecutableFunctionConstantValueVariableSize{l};
          }) | std::ranges::to<std::vector>())
    {}

    std::unique_ptr<Function> argmax;
    std::vector<ExecutableFunctionConstantValueVariableSize> labels;
    ExecutableFunctionConstantValueVariableSize invalid{"INVALID"};
};

} // namespace NES::Runtime::Execution::Functions

namespace NES::Runtime::Execution::Functions::ExecutableFunctionGeneratedRegistrar {
ExecutableFunctionRegistryReturnType RegisterKeywordSpottingLabelExecutableFunction(ExecutableFunctionRegistryArguments args) {
    if (args.childFunctions.size() != 1)
        throw TypeInferenceException("Function expects 1 argument");
    return std::make_unique<NES::Runtime::Execution::Functions::KeywordSpottingLabelExecutableFunction>(
        std::move(args.childFunctions[0]));
}
}
