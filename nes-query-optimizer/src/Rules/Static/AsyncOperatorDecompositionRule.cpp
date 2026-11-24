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

#include <Rules/Static/AsyncOperatorDecompositionRule.hpp>

#include <algorithm>
#include <any>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <Async/HandoffChannel.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Operators/AsynchronousOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/HandoffSinkConfig.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncSourceConfig.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Util/Any.hpp>
#include <AsyncOperatorLoweringRegistry.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES
{
LogicalOperator AsyncOperatorDecompositionRule::splitInvocation(const LogicalOperator& asyncOp, std::vector<LogicalOperator>& roots) const
{
    const auto lowering = lowerAsyncOperator(asyncOp);
    const auto child = asyncOp.getChildren().at(0);
    const auto node = asyncOp.getTraitSet().get<PlacementTrait>()->onNode;
    if (asyncOp.getTraitSet().get<MemoryLayoutTypeTrait>()->memoryLayout != MemoryLayoutType::ROW_LAYOUT)
    {
        throw NotImplemented("Async operators currently require row layout");
    }
    const auto channel = std::make_shared<HandoffChannel>(configuration.channelCapacity.value_or(HandoffChannel::DEFAULT_CAPACITY));
    const auto outputFields = asyncOp.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields();

    /// Prefill the fields used for the async output with a reasonable sentinel
    std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
    const auto inputSchema = child.getOutputSchema();
    for (const auto& field : outputFields)
    {
        const Identifier name = field.getFullyQualifiedName();
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) schema lookup returns an optional
        if (const auto input = inputSchema[name])
        {
            projections.emplace_back(name, FieldAccessLogicalFunction{*input});
        }
        else
        {
            const auto type = field.getDataType();
            std::string placeholder = "0";
            if (type.isType(DataType::Type::VARSIZED))
            {
                placeholder.clear();
            }
            else if (type.isType(DataType::Type::BOOLEAN))
            {
                placeholder = "false";
            }
            else if (type.isType(DataType::Type::CHAR))
            {
                placeholder.assign(1, '\0');
            }
            projections.emplace_back(name, ConstantValueLogicalFunction{type, std::move(placeholder)});
        }
    }

    const auto traits = asyncOp.getTraitSet();
    const LogicalOperator padded
        = ProjectionLogicalOperator::create(child, std::move(projections), ProjectionLogicalOperator::Asterisk{false})
              ->withTraitSet(traits);

    AsyncCallGraphBuilder builder{outputFields};
    std::vector<AsyncCallArgument> arguments;
    for (const auto& argument : lowering.arguments)
    {
        if (const auto field = argument.tryGetAs<FieldAccessLogicalFunction>())
        {
            arguments.emplace_back(builder.input(field.value()->getField().getFullyQualifiedName()));
        }
        else if (const auto constant = argument.tryGetAs<ConstantValueLogicalFunction>())
        {
            auto value = RuntimeValue::fromLiteral(argument.getDataType(), constant.value()->getConstantValue());
            if (!value)
            {
                throw std::move(value).error();
            }
            arguments.emplace_back(std::move(*value));
        }
        else
        {
            throw InvalidQuerySyntax("Async operator arguments must be normalized to fields or constants before decomposition");
        }
    }
    const auto producer = builder.call(lowering.implementation, std::move(arguments));
    /// Export every result even if a later projection discards it because the invocation itself might have effects.
    size_t index = 0;
    for (const auto& output : lowering.implementation.getOutputSchema())
    {
        builder.exportOutput(producer->output(index), output.getFullyQualifiedName());
        ++index;
    }
    const auto graph = builder.build();

    AsyncSourceConfig sourceConfig{.channel = channel, .calls = graph};
    if (configuration.maxConcurrency.has_value())
    {
        sourceConfig.maxConcurrency = *configuration.maxConcurrency;
    }
    if (configuration.maxRetries.has_value())
    {
        sourceConfig.maxRetries = *configuration.maxRetries;
    }

    const auto handoffSinkDescriptor = sinkCatalog->createAnonymousSinkDescriptor(
        std::make_shared<const Schema<UnqualifiedUnboundField, Ordered>>(outputFields),
        GeneralSinkConfig{.host = Host(node.getRawValue())},
        PluginSinkConfiguration{Identifier::parse("Handoff"), ExplicitAny{std::any{HandoffSinkConfig{.channel = channel}}}},
        OutputFormatterDescriptor{Identifier::parse("NATIVE"), ExplicitAny{std::any{std::monostate{}}}});
    roots.emplace_back(SinkLogicalOperator::create(padded, handoffSinkDescriptor)->withTraitSet(traits).withInferredSchema());

    const auto asyncSourceDescriptor = sourceCatalog->getAnonymousSource(
        PhysicalSourceBuilder{
            GeneralSourceConfig{.host = Host(node.getRawValue()), .maxInflightBuffers = std::nullopt},
            PluginSourceConfiguration{Identifier::parse("Async"), ExplicitAny{std::any{std::move(sourceConfig)}}},
            InputFormatterDescriptor{Identifier::parse("NATIVE"), ExplicitAny{std::any{std::monostate{}}}}},
        outputFields);
    INVARIANT(asyncSourceDescriptor.has_value(), "Failed to add physical source for async operator");
    return SourceDescriptorLogicalOperator::create(asyncSourceDescriptor.value())->withTraitSet(traits);
}

void AsyncOperatorDecompositionRule::validateSharedSubplans(const LogicalPlan& plan)
{
    auto pending = plan.getRootOperators();
    std::unordered_set<LogicalOperator> visited;
    std::unordered_map<LogicalOperator, size_t> consumers;
    while (!pending.empty())
    {
        const auto op = pending.back();
        pending.pop_back();
        if (!visited.insert(op).second)
        {
            continue;
        }
        for (const auto& child : op.getChildren())
        {
            ++consumers[child];
            pending.push_back(child);
        }
    }
    for (const auto& [op, count] : consumers)
    {
        if (count > 1)
        {
            pending.push_back(op);
        }
    }
    visited.clear();
    while (!pending.empty())
    {
        const auto op = pending.back();
        pending.pop_back();
        if (!visited.insert(op).second)
        {
            continue;
        }
        if (op.tryGetAs<AsynchronousOperator>())
        {
            throw UnsupportedQuery(
                "Shared async subplans with multiple consumers are not supported; reuse named output fields within one branch");
        }
        const auto children = op.getChildren();
        pending.insert(pending.end(), children.begin(), children.end());
    }
}

LogicalPlan AsyncOperatorDecompositionRule::apply(LogicalPlan plan) const
{
    using Visitor = PlanVisitor<bool, bool>;
    std::vector<LogicalOperator> roots;
    Visitor visitor{
        [](const LogicalOperator& op, const std::vector<bool>& parentSharing) -> Visitor::DownResult
        {
            const bool shared = parentSharing.size() > 1 || std::ranges::any_of(parentSharing, [](const bool value) { return value; });
            if (op.tryGetAs<AsynchronousOperator>() && shared)
            {
                throw UnsupportedQuery(
                    "Shared async subplans with multiple consumers are not supported; reuse named output fields within one branch");
            }
            std::unordered_map<LogicalOperator, bool> children;
            for (const auto& child : op.getChildren())
            {
                children.emplace(child, shared);
            }
            return {.operatorContext = parentSharing.empty(), .downContexts = std::move(children)};
        },
        [this, &roots](LogicalOperator op, std::vector<LogicalOperator> children, const bool isRoot) -> Visitor::UpResult
        {
            op = op.withChildren(std::move(children));
            if (op.tryGetAs<AsynchronousOperator>())
            {
                op = splitInvocation(op, roots);
            }
            if (isRoot)
            {
                roots.emplace_back(op.withInferredSchema());
                return Visitor::UpResult{std::monostate{}};
            }
            return op;
        }};
    return visitor.apply(std::move(plan)).withRootOperators(roots);
}
}
