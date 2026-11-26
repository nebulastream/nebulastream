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
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <Async/HandoffChannel.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Operators/AsyncLoweringContext.hpp>
#include <Operators/AsynchronousOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Static/DecideFieldMappings.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/HandoffSinkConfig.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncSourceConfig.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Any.hpp>
#include <AsyncOperatorLoweringRegistry.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES
{
namespace
{
using FieldName = QualifiedIdentifierBase<1>;
using Fields = std::unordered_map<FieldName, AsyncInputAccessor>;
/// A surviving entry-field name, or a value produced inside the region.
using Origin = std::optional<FieldName>;
using Origins = std::unordered_map<FieldName, Origin>;

bool isAliasProjection(const LogicalOperator& op)
{
    const auto projection = op.tryGetAs<ProjectionLogicalOperator>();
    return projection
        && std::ranges::all_of(projection.value()->getProjections(),
                               [](const auto& entry) { return entry.second.template tryGetAs<FieldAccessLogicalFunction>().has_value(); });
}

/// The map is scoped to the projection's child schema, never to globally reused result names.
template <typename Value>
std::unordered_map<FieldName, Value>
projectFields(const ProjectionLogicalOperator& projection, const std::unordered_map<FieldName, Value>& input)
{
    auto result = projection.hasAsterisk() ? input : std::unordered_map<FieldName, Value>{};
    for (const auto& [output, expression] : projection.getProjections())
    {
        const auto& source = expression.template getAs<FieldAccessLogicalFunction>()->getField().getFullyQualifiedName();
        result.insert_or_assign(output.getFullyQualifiedName(), input.at(source));
    }
    return result;
}

Schema<UnqualifiedUnboundField, Ordered> orderedFields(const LogicalOperator& op)
{
    return op.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields();
}

Field inputField(const LogicalOperator& op, const FieldName& name)
{
    const auto field = op.getOutputSchema().getFieldByName(name);
    if (!field)
    {
        throw CannotInferSchema("Async region input field {} is absent", name);
    }
    return *field;
}

TraitSet schemaTraits(const LogicalOperator& op, const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    auto traits
        = TraitSet{op.getTraitSet()
                   | std::views::filter(
                       [](const Trait& trait)
                       { return trait.getTypeInfo() != typeid(FieldMappingTrait) && trait.getTypeInfo() != typeid(FieldOrderingTrait); })};
    traits.insert(FieldOrderingTrait{schema});
    return traits;
}

LogicalOperator
withSchemaTraits(LogicalOperator op, const LogicalOperator& original, const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    return DecideFieldMappings::withInferredFieldMapping(op.withTraitSet(schemaTraits(original, schema)));
}

std::vector<LogicalOperator> collectRegion(const LogicalOperator& root)
{
    if (!root.tryGetAs<AsynchronousOperator>() && !isAliasProjection(root))
    {
        return {};
    }
    const auto placement = root.getTraitSet().tryGet<PlacementTrait>();
    const auto layout = root.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    if (!placement || !layout)
    {
        return {};
    }
    std::vector<LogicalOperator> region;
    auto op = root;
    while (op.getChildren().size() == 1 && (op.tryGetAs<AsynchronousOperator>() || isAliasProjection(op)))
    {
        const auto nextPlacement = op.getTraitSet().tryGet<PlacementTrait>();
        const auto nextLayout = op.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
        if (!nextPlacement || !nextLayout || nextPlacement.value()->onNode != placement.value()->onNode
            || nextLayout.value()->memoryLayout != layout.value()->memoryLayout)
        {
            break;
        }
        region.push_back(op);
        op = op.getChildren().front();
    }
    /// Trailing projections below the lowest async operator belong to the upstream synchronous pipeline
    /// (e.g., source column pruning) and must not be absorbed into the handoff schema.
    while (!region.empty() && !region.back().tryGetAs<AsynchronousOperator>())
    {
        region.pop_back();
    }
    return region;
}

LogicalFunction placeholder(const DataType& type)
{
    std::string literal = "0";
    if (type.isType(DataType::Type::VARSIZED))
    {
        literal.clear();
    }
    else if (type.isType(DataType::Type::BOOLEAN))
    {
        literal = "false";
    }
    else if (type.isType(DataType::Type::CHAR))
    {
        literal.assign(1, '\0');
    }
    return ConstantValueLogicalFunction{type, std::move(literal)};
}
}

LogicalOperator AsyncOperatorDecompositionRule::splitRegion(const std::vector<LogicalOperator>& region,
                                                            LogicalOperator input,
                                                            std::vector<LogicalOperator>& roots) const
{
    const auto& top = region.front();
    const auto node = top.getTraitSet().get<PlacementTrait>()->onNode;
    if (top.getTraitSet().get<MemoryLayoutTypeTrait>()->memoryLayout != MemoryLayoutType::ROW_LAYOUT)
    {
        throw NotImplemented("Async operators currently require row layout");
    }
    const auto inputOrder = orderedFields(input);
    const auto outputOrder = orderedFields(top);

    /// First determine escaping field provenance without constructing any native implementation.
    /// This lets input slots be renamed before a result reuses their logical names or changes their types.
    Origins origins;
    for (const auto& field : input.getOutputSchema())
    {
        origins.emplace(field.getFullyQualifiedName(), field.getFullyQualifiedName());
    }
    for (const auto& op : region | std::views::reverse)
    {
        if (const auto projection = op.tryGetAs<ProjectionLogicalOperator>())
        {
            origins = projectFields(*projection.value(), origins);
        }
        else
        {
            const auto childSchema = op.getChildren().front().getOutputSchema();
            for (const auto& field : op.getOutputSchema())
            {
                if (!childSchema.contains(field.getFullyQualifiedName()))
                {
                    origins.emplace(field.getFullyQualifiedName(), std::nullopt);
                }
            }
        }
    }

    std::unordered_set<FieldName> reserved;
    for (const auto& field : inputOrder)
    {
        reserved.insert(field.getFullyQualifiedName());
    }
    for (const auto& field : outputOrder)
    {
        reserved.insert(field.getFullyQualifiedName());
    }
    std::unordered_map<FieldName, FieldName> physicalInputs;
    std::vector<ProjectionLogicalOperator::UnboundProjection> snapshots;
    std::vector<UnqualifiedUnboundField> snapshotFields;
    size_t nextInput = 0;
    bool renamed = false;
    for (const auto& field : inputOrder)
    {
        const auto& name = field.getFullyQualifiedName();
        auto physical = name;
        if (const auto found = origins.find(name); found != origins.end())
        {
            const auto& forwarded = found->second;
            if (!forwarded || *forwarded != name)
            {
                do
                {
                    physical = QualifiedIdentifier::create(Identifier::parse("__async_input_" + std::to_string(nextInput++)));
                } while (!reserved.insert(physical).second);
                renamed = true;
            }
        }
        physicalInputs.emplace(name, physical);
        snapshots.emplace_back(physical, FieldAccessLogicalFunction{inputField(input, name)});
        snapshotFields.emplace_back(physical, field.getDataType());
    }
    const auto inputMapping = input.getTraitSet().get<FieldMappingTrait>();
    const bool hasPhysicalAliases
        = std::ranges::any_of(input.getOutputSchema(),
                              [&](const Field& field)
                              {
                                  const auto physical = inputMapping->getMapping(field.unbound());
                                  return !physical || *physical != field.getFullyQualifiedName();
                              });
    if (renamed || hasPhysicalAliases)
    {
        input = withSchemaTraits(ProjectionLogicalOperator::create(input, std::move(snapshots), ProjectionLogicalOperator::Asterisk{false}),
                                 top,
                                 Schema<UnqualifiedUnboundField, Ordered>{snapshotFields});
    }

    /// The paired endpoints have exactly the same physical schema. Internal producer values have no tuple slots.
    std::vector<UnqualifiedUnboundField> physicalFields{outputOrder.begin(), outputOrder.end()};
    std::unordered_set<FieldName> physicalNames;
    for (const auto& field : physicalFields)
    {
        physicalNames.insert(field.getFullyQualifiedName());
    }
    for (const auto& field : snapshotFields)
    {
        if (physicalNames.insert(field.getFullyQualifiedName()).second)
        {
            physicalFields.push_back(field);
        }
    }
    const Schema<UnqualifiedUnboundField, Ordered> physicalSchema{physicalFields};
    AsyncCallGraphBuilder builder{physicalSchema};
    Fields fields;
    for (const auto& [logical, physical] : physicalInputs)
    {
        fields.emplace(logical, AsyncInputAccessor{builder.input(physical)});
    }
    for (const auto& op : region | std::views::reverse)
    {
        if (const auto projection = op.tryGetAs<ProjectionLogicalOperator>())
        {
            fields = projectFields(*projection.value(), fields);
            continue;
        }
        AsyncLoweringContext context{fields};
        auto implementation = lowerAsyncOperator(op, context);
        const auto declared = implementation.getWrittenFields();
        const auto producer = builder.call(std::move(implementation), context.getDependencies());
        /// An operator invocation is an effect root even when no result leaves this region.
        builder.require(producer);
        const auto childSchema = op.getChildren().front().getOutputSchema();
        /// Written fields are not the full row schema. For now every write must append a new
        /// field; supporting replacement later also requires tracking overwritten field provenance.
        if (op.getOutputSchema().size() != childSchema.size() + declared.size())
        {
            throw CannotInferSchema("Async operators currently must append their written fields to the input schema");
        }
        for (const auto& inputField : childSchema)
        {
            const auto forwarded = op.getOutputSchema().getFieldByName(inputField.getFullyQualifiedName());
            if (!forwarded || forwarded->getDataType() != inputField.getDataType())
            {
                throw CannotInferSchema("Async operators currently must preserve all input fields unchanged");
            }
        }
        size_t index = 0;
        for (const auto& result : declared)
        {
            if (childSchema.contains(result.getFullyQualifiedName()))
            {
                throw CannotInferSchema("Async operators currently cannot overwrite input field {}", result.getFullyQualifiedName());
            }
            const auto logical = op.getOutputSchema().getFieldByName(result.getFullyQualifiedName());
            if (!logical || logical->getDataType() != result.getDataType())
            {
                throw CannotInferSchema("Async lowering must preserve its declared result names and types");
            }
            fields.emplace(result.getFullyQualifiedName(), AsyncInputAccessor{producer->output(index++)});
        }
    }
    std::vector<ProjectionLogicalOperator::UnboundProjection> padding;
    for (const auto& field : physicalSchema)
    {
        const auto& name = field.getFullyQualifiedName();
        const auto final = origins.find(name);
        if (final != origins.end())
        {
            if (const auto& source = final->second; source)
            {
                if (physicalInputs.at(*source) != name)
                {
                    padding.emplace_back(name, FieldAccessLogicalFunction{inputField(input, physicalInputs.at(*source))});
                }
            }
            else
            {
                padding.emplace_back(name, placeholder(field.getDataType()));
                const auto& accessor = fields.at(name);
                builder.exportOutput(std::get<AsyncResultReference>(accessor.getSource()), name);
            }
        }
    }
    /// Preserve unchanged inputs through the normal asterisk path, rather than generating one
    /// physical map per forwarded field at every async boundary. A projection with no explicit
    /// expressions would have no physical root after a non-source child, so retain an ordinary
    /// materialization when all results are internal or discarded.
    const bool forwardInputs = !padding.empty();
    if (!forwardInputs)
    {
        for (const auto& field : physicalSchema)
        {
            const auto& name = field.getFullyQualifiedName();
            padding.emplace_back(name, FieldAccessLogicalFunction{inputField(input, name)});
        }
    }
    const auto padded
        = withSchemaTraits(ProjectionLogicalOperator::create(input, std::move(padding), ProjectionLogicalOperator::Asterisk{forwardInputs}),
                           top,
                           physicalSchema);
    const auto channel = std::make_shared<HandoffChannel>(configuration.channelCapacity.value_or(HandoffChannel::DEFAULT_CAPACITY));
    AsyncSourceConfig sourceConfig{.channel = channel, .calls = builder.build()};
    if (configuration.maxConcurrency)
    {
        sourceConfig.maxConcurrency = *configuration.maxConcurrency;
    }
    const auto sinkDescriptor = sinkCatalog->createAnonymousSinkDescriptor(
        std::make_shared<const Schema<UnqualifiedUnboundField, Ordered>>(physicalSchema),
        GeneralSinkConfig{.host = Host(node.getRawValue())},
        PluginSinkConfiguration{Identifier::parse("Handoff"), ExplicitAny{std::any{HandoffSinkConfig{.channel = channel}}}},
        OutputFormatterDescriptor{Identifier::parse("NATIVE"), ExplicitAny{std::any{std::monostate{}}}});
    roots.push_back(SinkLogicalOperator::create(padded, sinkDescriptor)->withTraitSet(top.getTraitSet()).withInferredSchema());
    const auto descriptor = sourceCatalog->getAnonymousSource(
        PhysicalSourceBuilder{GeneralSourceConfig{.host = Host(node.getRawValue()), .maxInflightBuffers = std::nullopt},
                              PluginSourceConfiguration{Identifier::parse("Async"), ExplicitAny{std::any{std::move(sourceConfig)}}},
                              InputFormatterDescriptor{Identifier::parse("NATIVE"), ExplicitAny{std::any{std::monostate{}}}}},
        physicalSchema);
    INVARIANT(descriptor.has_value(), "Failed to add physical source for async region");
    const auto source = withSchemaTraits(SourceDescriptorLogicalOperator::create(*descriptor), top, physicalSchema);
    if (physicalSchema == outputOrder)
    {
        return source;
    }
    std::vector<ProjectionLogicalOperator::UnboundProjection> restored;
    for (const auto& field : outputOrder)
    {
        const auto& name = field.getFullyQualifiedName();
        restored.emplace_back(name, FieldAccessLogicalFunction{source.getOutputSchema().getFieldByName(name).value()});
    }
    return withSchemaTraits(
        ProjectionLogicalOperator::create(source, std::move(restored), ProjectionLogicalOperator::Asterisk{false}), top, outputOrder);
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
    validateSharedSubplans(plan);
    std::vector<LogicalOperator> roots;
    std::unordered_map<LogicalOperator, LogicalOperator> rewritten;
    const auto rewrite = [&](this const auto& self, const LogicalOperator& op) -> LogicalOperator
    {
        if (const auto found = rewritten.find(op); found != rewritten.end())
        {
            return found->second;
        }
        LogicalOperator result = op;
        const auto region = collectRegion(op);
        if (!region.empty())
        {
            result = splitRegion(region, self(region.back().getChildren().front()), roots);
        }
        else
        {
            std::vector<LogicalOperator> children;
            for (const auto& child : op.getChildren())
            {
                children.push_back(self(child));
            }
            result = op.withChildren(std::move(children)).withInferredSchema();
        }
        rewritten.emplace(op, result);
        return result;
    };
    for (const auto& root : plan.getRootOperators())
    {
        roots.push_back(rewrite(root));
    }
    return plan.withRootOperators(roots);
}
}
