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

#include <Rules/Static/HiPyChainRule.hpp>

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UDFCallLogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Static/UDFLiftingRule.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <nlohmann/json.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>
#include <UdfBridgeRegistry.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> HiPyChainRule::needs() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> HiPyChainRule::neededBy() const
{
    return {typeid(UDFLiftingRule)};
}

namespace
{
DataType fieldType(const std::string& name, const bool nullable)
{
    const auto type = magic_enum::enum_cast<DataType::Type>(name);
    INVARIANT(type.has_value(), "nes_hipy_split.py names an NES type: {}", name);
    return DataType{type.value(), nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE};
}

/// The chain of a HiPy UDF call, if nes_hipy_split.py could lower it.
std::optional<nlohmann::json> chainOf(const UDFCallLogicalFunction& call)
{
    const auto& descriptor = call.getDescriptor();
    if (!descriptor.has_value() || descriptor->getExecution() != UdfExecution::Inline || descriptor->getCompiler() != "HiPy"
        || !descriptor->getCompiled().has_value())
    {
        return std::nullopt;
    }
    auto compiled = nlohmann::json::parse(descriptor->getCompiled()->pipelineBitcode, nullptr, false);
    if (compiled.is_discarded() || !compiled.contains("chain") || !compiled["chain"].value("lowerable", false))
    {
        return std::nullopt;
    }
    return std::move(compiled["chain"]);
}

/// The expression that computes `call` through its stages: one call per stage output, whose arguments are the calls (or the
/// arguments of `call`) that compute the stage's inputs.
LogicalFunction lowerChain(const UDFCallLogicalFunction& call, const nlohmann::json& chain, const std::filesystem::path& bridge)
{
    const auto& descriptor = call.getDescriptor().value();
    std::unordered_map<int64_t, LogicalFunction> values;
    std::unordered_map<int64_t, DataType> declared;
    for (const auto& [index, argument] : std::views::enumerate(chain.at("arguments")))
    {
        const auto id = argument.at("id").get<int64_t>();
        values.emplace(id, call.getChildren().at(static_cast<size_t>(index)));
        declared.emplace(id, descriptor.getArgTypes().at(static_cast<size_t>(index)));
    }
    /// The stage call that computes each value.
    std::unordered_map<int64_t, std::pair<size_t, const nlohmann::json*>> producers;
    for (const auto& [index, stage] : std::views::enumerate(chain.at("stages")))
    {
        for (const auto& [output, stageCall] : stage.at("calls").items())
        {
            producers.emplace(std::stoll(output), std::make_pair(static_cast<size_t>(index), &stageCall));
        }
    }

    std::function<LogicalFunction(int64_t)> value = [&](const int64_t id) -> LogicalFunction
    {
        if (const auto found = values.find(id); found != values.end())
        {
            return found->second;
        }
        const auto [stageIndex, stageCall] = producers.at(id);
        const auto name = fmt::format("{}__s{}_v{}", call.getUdfName(), stageIndex, id);
        std::vector<LogicalFunction> arguments;
        std::vector<DataType> argumentTypes;
        for (const auto& [position, input] : std::views::enumerate(stageCall->at("arguments")))
        {
            const auto inputId = input.get<int64_t>();
            arguments.push_back(value(inputId));
            const auto notNull = stageCall->at("notNull").at(static_cast<size_t>(position)).get<bool>();
            auto type = declared.contains(inputId) ? declared.at(inputId)
                                                   : fieldType(stageCall->at("argumentTypes").at(static_cast<size_t>(position)), true);
            type.nullable = !notNull;
            argumentTypes.push_back(type);
        }
        auto returnType = id == chain.at("result").get<int64_t>() ? descriptor.getReturnType()
                                                                   : fieldType(stageCall->at("returnType").get<std::string>(), true);
        returnType.nullable = true;
        std::optional<UdfDescriptor> stageDescriptor;
        if (stageCall->at("kind") == "native")
        {
            const auto program = stageCall->at("program").get<std::string>();
            stageDescriptor = UdfDescriptor::fromCompiled(
                name,
                argumentTypes,
                returnType,
                "HiPy",
                CompiledInlineUdf{
                    .symbol = nlohmann::json::parse(program).at("entry").get<std::string>(), .pipelineBitcode = program, .interpreterBitcode = {}});
        }
        else
        {
            stageDescriptor = UdfDescriptor::fromSource(
                name,
                bridge,
                InlineUdfSource{
                    .parameterNames = stageCall->at("parameterNames").get<std::vector<std::string>>(),
                    .body = stageCall->at("body").get<std::string>()},
                argumentTypes,
                returnType,
                UdfExecution::InProcess,
                UdfLanguage::Python);
        }
        LogicalFunction lowered = UDFCallLogicalFunction(name, std::move(stageDescriptor).value(), std::move(arguments));
        values.emplace(id, lowered);
        return lowered;
    };
    return value(chain.at("result").get<int64_t>());
}

class Splitter
{
public:
    LogicalFunction rewrite(const LogicalFunction& function, const Schema<Field, Unordered>& schema)
    {
        auto rewritten = function.withChildren(
            function.getChildren() | std::views::transform([&](const LogicalFunction& child) { return rewrite(child, schema); })
            | std::ranges::to<std::vector>());
        const auto call = rewritten.tryGetAs<UDFCallLogicalFunction>();
        if (!call.has_value())
        {
            return rewritten;
        }
        const auto chain = chainOf(*call.value());
        if (!chain.has_value())
        {
            return rewritten;
        }
        if (bridge.empty())
        {
            bridge = resolveUdfBridgeLibraryPath(UdfLanguage::Python, UdfExecution::InProcess);
        }
        changed = true;
        return lowerChain(*call.value(), chain.value(), bridge).withInferredDataType(schema);
    }

    LogicalOperator split(const LogicalOperator& current)
    {
        changed = false;
        if (const auto selection = current.tryGetAs<SelectionLogicalOperator>())
        {
            const auto child = selection.value()->getChild();
            auto predicate = rewrite(selection.value()->getPredicate(), child.getOutputSchema());
            if (changed)
            {
                return SelectionLogicalOperator::create(child, std::move(predicate)).withTraitSet(selection.value()->getTraitSet());
            }
        }
        if (const auto projection = current.tryGetAs<ProjectionLogicalOperator>())
        {
            const auto child = projection.value()->getChild();
            auto projections = projection.value()->getUnboundProjections();
            for (auto& [name, function] : projections)
            {
                function = rewrite(function, child.getOutputSchema());
            }
            if (changed)
            {
                return ProjectionLogicalOperator::create(
                           child, std::move(projections), ProjectionLogicalOperator::Asterisk{projection.value()->hasAsterisk()})
                    .withTraitSet(projection.value()->getTraitSet());
            }
        }
        return current;
    }

private:
    bool changed = false;
    std::filesystem::path bridge;
};
}

LogicalPlan HiPyChainRule::apply(LogicalPlan queryPlan) const /// NOLINT(readability-convert-member-functions-to-static)
{
    /// NOLINTNEXTLINE(concurrency-mt-unsafe)
    if (const auto* enabled = std::getenv("NES_HIPY_SPLIT"); enabled != nullptr && std::string_view{enabled} == "0")
    {
        return queryPlan;
    }
    Splitter splitter;
    PlanVisitor<> visitor{PlanVisitor<>::FunctionUp{
        [&splitter](
            const LogicalOperator& op,
            std::vector<LogicalOperator> children,
            std::monostate,
            const std::unordered_map<LogicalOperator, std::monostate>&) -> PlanVisitor<>::UpResult
        { return splitter.split(op.withChildren(std::move(children))); }}};
    return visitor.apply(std::move(queryPlan));
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType HiPyChainRule::create(PlanRuleRegistryArguments)
{
    return HiPyChainRule{};
}

}
