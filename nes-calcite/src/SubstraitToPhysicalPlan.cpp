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

#include <Calcite/CalcitePlanner.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/NullValuePhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Watermark/EventTimeWatermarkAssignerPhysicalOperator.hpp>
#include <Watermark/TimeFunction.hpp>
#include <fmt/format.h>
#include <substrait/algebra.pb.h>
#include <substrait/plan.pb.h>
#include "SubstraitTypes.hpp"
#include <ErrorHandling.hpp>
#include <MapPhysicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <InputFormatterProvider.hpp>
#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationProbePhysicalOperator.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <HashMapSlice.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <WindowBasedOperatorHandler.hpp>
#include <magic_enum/magic_enum.hpp>
#include <InputFormatterDescriptor.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <PipelinedQueryPlan.hpp>
#include <Pipeline.hpp>
#include <ScanPhysicalOperator.hpp>
#include <EmitPhysicalOperator.hpp>
#include <EmitOperatorHandler.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <SelectionPhysicalOperator.hpp>
#include <SinkPhysicalOperator.hpp>
#include <SourceDescriptorPhysicalOperator.hpp>
#include <nes_calcite_planner.pb.h>

/// Builds the pipelines of a physical plan as Calcite planned them (see NesPhysical.java): every decision, down to field
/// names, origin ids and pipelines, is in the PhysicalOperator payloads; this only builds what they describe.
namespace NES::Calcite
{
namespace
{
using PhysicalSchema = Schema<QualifiedUnboundField, Ordered>;
constexpr auto ROW = MemoryLayoutType::ROW_LAYOUT;

/// The pipelines of a physical plan, built as its payloads describe them.
class Converter
{
public:
    Converter(
        const substrait::Plan& plan,
        const PhysicalPlanContext& context,
        Host worker,
        const QueryId queryId,
        const QueryExecutionConfiguration& configuration)
        : context(context)
        , worker(std::move(worker))
        , bufferSize(configuration.operatorBufferSize)
        , numberOfPartitions(configuration.numberOfPartitions)
        , pageSize(configuration.pageSize)
        , sliceCacheConfiguration(configuration.sliceCacheConfiguration)
        , functions(detail::functionNames(plan))
        , userDefinedTypes(detail::unsignedTypes(plan))
        , result(std::make_shared<PipelinedQueryPlan>(
              queryId, ExecutionConfiguration().withExecutionMode(configuration.executionMode).withNautilusInlining(configuration.nautilusInlining)))
    {
    }

    /// The parts of the plan that run on this worker (one Substrait root each), as one plan.
    std::shared_ptr<PipelinedQueryPlan> convert(const substrait::Plan& plan)
    {
        for (const auto& relation : plan.relations())
        {
            if (not relation.has_root())
            {
                throw UnsupportedQuery("a physical plan has only root relations");
            }
            const auto& root = relation.root().input();
            const auto last = payload(root).first;
            if (Host(last.host()) != worker)
            {
                continue;
            }
            /// The sink is bound first: the Emit before it writes in its output format.
            const auto outputSchema = unqualified(schema(last.output_schema()));
            if (last.has_sink())
            {
                sinkDescriptor = context.bindSink(outputSchema);
            }
            else if (last.has_network_sink())
            {
                sinkDescriptor = context.sinkCatalog.getAnonymousSink(
                    outputSchema,
                    Identifier::parse("Network"),
                    worker,
                    {{Identifier::parse("channel"), last.network_sink().channel()},
                     {Identifier::parse("bind"), dataAddress(worker)},
                     {Identifier::parse("data_endpoint"), dataAddress(Host(last.network_sink().downstream_host()))},
                     {Identifier::parse("output_format"), "NATIVE"}},
                    {});
                if (not sinkDescriptor)
                {
                    throw UnsupportedQuery("cannot create the network sink of channel {}", last.network_sink().channel());
                }
            }
            else
            {
                throw UnsupportedQuery("a part of a physical plan ends in a sink, not in {}", last.ShortDebugString());
            }
            convertOperator(root);
        }
        return result;
    }

private:
    struct Converted
    {
        uint32_t pipeline;
        PhysicalSchema outputSchema;
        std::optional<SourceDescriptor> source; /// for a SourceScan: what a Scan after it parses
    };

    const PhysicalPlanContext& context;
    Host worker;
    uint64_t bufferSize;
    uint64_t numberOfPartitions;
    uint64_t pageSize;
    SliceCacheConfiguration sliceCacheConfiguration;
    std::unordered_map<uint32_t, std::string> functions;
    detail::UserDefinedTypes userDefinedTypes;
    std::shared_ptr<PipelinedQueryPlan> result;
    std::optional<SinkDescriptor> sinkDescriptor;
    std::unordered_map<uint32_t, std::shared_ptr<Pipeline>> pipelines;

    /// A window's state, which its builds use before its probe creates the handler that owns it.
    struct Window
    {
        OperatorHandlerId handlerId;
        std::unique_ptr<DefaultTimeBasedSliceStore> store;
        std::vector<uint32_t> buildPipelines;
    };
    std::unordered_map<uint32_t, Window> windows;

    Window& window(const nes::calcite::PhysicalOperator::Window& spec)
    {
        auto found = windows.find(spec.handler());
        if (found == windows.end())
        {
            found = windows
                        .emplace(
                            spec.handler(),
                            Window{
                                .handlerId = getNextOperatorHandlerId(),
                                .store = std::make_unique<DefaultTimeBasedSliceStore>(spec.size_ms(), spec.slide_ms(), sliceCacheConfiguration),
                                .buildPipelines = {}})
                        .first;
        }
        return found->second;
    }

    /// The aggregation functions of a window: arguments from `input`, results named and typed as in the probe's schema.
    static std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationFunctions(
        const google::protobuf::RepeatedPtrField<nes::calcite::PhysicalOperator::WindowAggregation>& aggregations,
        const PhysicalSchema& input,
        const PhysicalSchema& probe,
        const size_t keys)
    {
        std::vector<std::shared_ptr<AggregationPhysicalFunction>> result;
        const auto layout = std::make_shared<DefaultPagedVectorTupleLayout>(input);
        for (int i = 0; i < aggregations.size(); ++i)
        {
            const auto& aggregation = aggregations.Get(i);
            const auto& output = *std::ranges::next(probe.begin(), static_cast<std::ptrdiff_t>(2 + keys + i));
            const auto& argument = *std::ranges::next(input.begin(), aggregation.field());
            const auto name = nes::calcite::WindowAggregate::Function_Name(aggregation.function());
            static const std::unordered_map<std::string, std::string> registryNames{
                {"SUM", "Sum"}, {"COUNT", "Count"}, {"MIN", "Min"}, {"MAX", "Max"}, {"AVG", "Avg"}};
            const auto registered = registryNames.find(name);
            const auto factory
                = registered == registryNames.end() ? std::nullopt : AggregationPhysicalFunctionRegistry::instance().find(registered->second);
            if (not factory)
            {
                throw UnsupportedQuery("unknown window aggregation {}", name);
            }
            result.push_back((*factory)(AggregationPhysicalFunctionRegistryArguments{
                .inputType = argument.getDataType(),
                .resultType = output.getDataType(),
                .inputFunction = FieldAccessPhysicalFunction(argument.getFullyQualifiedName()),
                .resultFieldIdentifier = output.getFullyQualifiedName(),
                .tupleLayout = layout,
                .includeNullValues = aggregation.include_nulls()}));
        }
        return result;
    }

    /// The hash map of a window's state: an entry per key (the probe's key fields), followed by the aggregation states.
    ChainedHashMapConfig hashMapConfig(
        const PhysicalSchema& probe, const size_t keys, const std::vector<std::shared_ptr<AggregationPhysicalFunction>>& aggregations) const
    {
        const auto fields = names(probe);
        const std::vector<QualifiedIdentifier> keyNames{fields.begin() + 2, fields.begin() + 2 + static_cast<std::ptrdiff_t>(keys)};
        const std::vector<QualifiedIdentifier> valueNames{fields.begin() + 2 + static_cast<std::ptrdiff_t>(keys), fields.end()};
        auto [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(probe, keyNames, valueNames);
        uint64_t keySize = 0;
        for (const auto& key : fieldKeys)
        {
            keySize += key.type.getSizeInBytesWithNull();
        }
        uint64_t valueSize = 0;
        for (const auto& aggregation : aggregations)
        {
            valueSize += aggregation->getSizeOfStateInBytes();
        }
        return ChainedHashMapConfig{
            .entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize,
            .numberOfBuckets = numberOfPartitions,
            .pageSize = pageSize,
            .bloomFilterParams = std::nullopt,
            .fieldKeys = std::move(fieldKeys),
            .fieldValues = std::move(fieldValues),
            .hashFunction = std::make_shared<MurMur3HashFunction>()};
    }

    static const QualifiedUnboundField& fieldAt(const PhysicalSchema& schema, const uint32_t index)
    {
        return *std::ranges::next(schema.begin(), index);
    }

    static std::pair<nes::calcite::PhysicalOperator, std::vector<const substrait::Rel*>> payload(const substrait::Rel& rel)
    {
        const auto [detail, inputs] = [&]() -> std::pair<google::protobuf::Any, std::vector<const substrait::Rel*>>
        {
            switch (rel.rel_type_case())
            {
                case substrait::Rel::kExtensionLeaf:
                    return {rel.extension_leaf().detail(), {}};
                case substrait::Rel::kExtensionSingle:
                    return {rel.extension_single().detail(), {&rel.extension_single().input()}};
                case substrait::Rel::kExtensionMulti:
                    return {
                        rel.extension_multi().detail(),
                        rel.extension_multi().inputs() | std::views::transform([](const auto& input) { return &input; })
                            | std::ranges::to<std::vector>()};
                default:
                    throw UnsupportedQuery("a physical plan holds only physical operators, not {}", rel.ShortDebugString());
            }
        }();
        nes::calcite::PhysicalOperator physical;
        if (not detail.UnpackTo(&physical))
        {
            throw UnsupportedQuery("unknown physical operator {}", detail.type_url());
        }
        return {std::move(physical), inputs};
    }

    /// A pipeline's first operator opens it under the payload's pipeline id.
    std::shared_ptr<Pipeline> open(const uint32_t id, std::shared_ptr<Pipeline> pipeline)
    {
        pipelines.emplace(id, pipeline);
        return pipeline;
    }

    /// Inputs in other pipelines feed this one.
    void link(const std::vector<Converted>& inputs, const uint32_t id)
    {
        for (const auto& input : inputs)
        {
            if (input.pipeline != id)
            {
                const auto& predecessor = pipelines.at(input.pipeline);
                predecessor->addSuccessor(pipelines.at(id), predecessor);
            }
        }
    }

    Converted convertOperator(const substrait::Rel& rel)
    {
        const auto [physical, inputRels] = payload(rel);
        std::vector<Converted> inputs;
        for (const auto* input : inputRels)
        {
            inputs.push_back(convertOperator(*input));
        }
        const auto id = physical.pipeline();
        auto outputSchema = schema(physical.output_schema());

        switch (physical.operator_case())
        {
            case nes::calcite::PhysicalOperator::kSourceScan: {
                const auto& scan = physical.source_scan();
                auto descriptor = context.sourceCatalog.getPhysicalSource(PhysicalSourceId(scan.physical_source_id()));
                if (not descriptor)
                {
                    throw UnsupportedQuery("unknown physical source {}", scan.physical_source_id());
                }
                result->addPipeline(open(id, std::make_shared<Pipeline>(SourceDescriptorPhysicalOperator(*descriptor, OriginId(scan.origin_id())))));
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::move(descriptor)};
            }
            case nes::calcite::PhysicalOperator::kScan: {
                const auto fields = names(outputSchema);
                auto scan = [&]
                {
                    if (physical.scan().parses_source())
                    {
                        const auto& source = inputs.at(0).source.value();
                        return ScanPhysicalOperator(
                            provideInputFormatter(
                                source.getInputFormatterDescriptor(), LowerSchemaProvider::lowerSchema(bufferSize, source.getSchema(), ROW)),
                            fields);
                    }
                    return ScanPhysicalOperator(LowerSchemaProvider::lowerSchema(bufferSize, outputSchema, ROW), fields);
                }();
                open(id, std::make_shared<Pipeline>(PhysicalOperator(scan)));
                link(inputs, id);
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::nullopt};
            }
            case nes::calcite::PhysicalOperator::kSelection: {
                const auto& input = inputs.at(0);
                pipelines.at(id)->appendOperator(SelectionPhysicalOperator(function(physical.selection().predicate(), input.outputSchema).first));
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::nullopt};
            }
            case nes::calcite::PhysicalOperator::kMap: {
                const auto& input = inputs.at(0);
                pipelines.at(id)->appendOperator(
                    MapPhysicalOperator(fieldName(physical.map().field()), function(physical.map().expression(), input.outputSchema).first));
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::nullopt};
            }
            case nes::calcite::PhysicalOperator::kEmit: {
                /// A native sink (e.g. a NetworkSink) takes rows as they are, as in the PipeliningPhase.
                const auto bufferRef = physical.emit().for_sink() and not sinkDescriptor->getOutputFormatterDescriptor().isNative()
                    ? LowerSchemaProvider::lowerSchemaWithOutputFormat(bufferSize, outputSchema, sinkDescriptor->getOutputFormatterDescriptor())
                    : LowerSchemaProvider::lowerSchema(bufferSize, outputSchema, ROW);
                const auto handlerId = getNextOperatorHandlerId();
                const auto& pipeline = pipelines.at(id);
                pipeline->getOperatorHandlers().emplace(handlerId, std::make_shared<EmitOperatorHandler>());
                pipeline->appendOperator(EmitPhysicalOperator(handlerId, bufferRef));
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::nullopt};
            }
            case nes::calcite::PhysicalOperator::kNetworkSource: {
                const auto& network = physical.network_source();
                auto descriptor = context.sourceCatalog.getAnonymousSource(
                    Identifier::parse("Network"),
                    unqualified(outputSchema),
                    worker,
                    {{InputFormatterDescriptor::TYPE_FIELD.getName(), "NATIVE"}},
                    {{Identifier::parse("channel"), network.channel()}, {Identifier::parse("bind"), dataAddress(worker)}});
                if (not descriptor)
                {
                    throw UnsupportedQuery("cannot create the network source of channel {}", network.channel());
                }
                result->addPipeline(open(id, std::make_shared<Pipeline>(SourceDescriptorPhysicalOperator(*descriptor, OriginId(network.origin_id())))));
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::move(descriptor)};
            }
            case nes::calcite::PhysicalOperator::kWatermarkAssigner: {
                const auto& time = fieldAt(inputs.at(0).outputSchema, physical.watermark_assigner().time_field());
                pipelines.at(id)->appendOperator(EventTimeWatermarkAssignerPhysicalOperator(
                    EventTimeFunction(FieldAccessPhysicalFunction(time.getFullyQualifiedName()), Windowing::TimeUnit::Milliseconds())));
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::nullopt};
            }
            case nes::calcite::PhysicalOperator::kWindowBuild: {
                const auto& spec = physical.window_build();
                const auto& input = inputs.at(0).outputSchema;
                const auto probe = schema(spec.probe_schema());
                auto& state = window(spec.window());
                const auto aggregations = aggregationFunctions(spec.aggregations(), input, probe, spec.key_fields_size());
                std::vector<PhysicalFunction> keys;
                for (const auto key : spec.key_fields())
                {
                    keys.emplace_back(FieldAccessPhysicalFunction(fieldAt(input, key).getFullyQualifiedName()));
                }
                const auto config = hashMapConfig(probe, spec.key_fields_size(), aggregations);
                auto sliceStoreRef = state.store->createSliceStoreRef(
                    [](Slice& slice, const WorkerThreadId workerThreadId, AbstractBufferProvider& bufferProvider) -> const TupleBuffer*
                    { return dynamic_cast<AggregationSlice&>(slice).getOrCreateHashMapBufferRefForWorker(bufferProvider, workerThreadId); },
                    [config](WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
                    { return handler.getCreateNewSlicesFunction(CreateNewHashMapSliceArgs{config, bufferProvider}); });
                pipelines.at(id)->appendOperator(AggregationBuildPhysicalOperator(
                    state.handlerId,
                    std::make_unique<EventTimeFunction>(
                        FieldAccessPhysicalFunction(fieldAt(input, spec.time_field()).getFullyQualifiedName()), Windowing::TimeUnit::Milliseconds()),
                    std::move(sliceStoreRef),
                    aggregations,
                    config,
                    std::move(keys)));
                state.buildPipelines.push_back(id);
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::nullopt};
            }
            case nes::calcite::PhysicalOperator::kWindowProbe: {
                const auto& spec = physical.window_probe();
                const auto keys = static_cast<size_t>(std::ranges::size(outputSchema)) - 2 - spec.aggregations_size();
                auto& state = window(spec.window());
                /// Its first input is a build, whose output is its input: the arguments' types come from there.
                const auto aggregations = aggregationFunctions(spec.aggregations(), inputs.at(0).outputSchema, outputSchema, keys);
                const auto config = hashMapConfig(outputSchema, keys, aggregations);
                const auto field = [&](const uint32_t index)
                {
                    const auto& f = fieldAt(outputSchema, index);
                    return UnqualifiedUnboundField(*std::ranges::prev(f.getFullyQualifiedName().end()), f.getDataType());
                };
                open(id, std::make_shared<Pipeline>(PhysicalOperator(
                             AggregationProbePhysicalOperator(config, aggregations, state.handlerId, WindowMetaData{field(0), field(1)}))));
                /// The handler owns the window's state; the builds and the probe reach it under the same id.
                const auto handler = std::make_shared<AggregationOperatorHandler>(
                    spec.input_origin_ids() | std::views::transform([](const auto origin) { return OriginId(origin); })
                        | std::ranges::to<std::vector>(),
                    OriginId(spec.output_origin_id()),
                    std::move(state.store));
                for (const auto pipeline : state.buildPipelines)
                {
                    pipelines.at(pipeline)->getOperatorHandlers().emplace(state.handlerId, handler);
                }
                pipelines.at(id)->getOperatorHandlers().emplace(state.handlerId, handler);
                link(inputs, id);
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::nullopt};
            }
            case nes::calcite::PhysicalOperator::kSink:
            case nes::calcite::PhysicalOperator::kNetworkSink: {
                open(id, std::make_shared<Pipeline>(SinkPhysicalOperator(*sinkDescriptor)));
                link(inputs, id);
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::nullopt};
            }
            default:
                throw UnsupportedQuery("unknown physical operator {}", physical.ShortDebugString());
        }
    }

    static QualifiedIdentifier fieldName(const std::string& name) { return QualifiedIdentifier::parse(name); }

    static Schema<UnqualifiedUnboundField, Ordered> unqualified(const PhysicalSchema& schema)
    {
        return schema
            | std::views::transform([](const auto& field)
                                    { return UnqualifiedUnboundField(*std::ranges::prev(field.getFullyQualifiedName().end()), field.getDataType()); })
            | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();
    }

    const std::string& dataAddress(const Host& host) const
    {
        const auto found = context.dataAddresses.find(host);
        if (found == context.dataAddresses.end())
        {
            throw UnsupportedQuery("worker {} has no data address", host);
        }
        return found->second;
    }

    static std::vector<QualifiedIdentifier> names(const PhysicalSchema& schema)
    {
        return schema | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); })
            | std::ranges::to<std::vector>();
    }

    PhysicalSchema schema(const google::protobuf::RepeatedPtrField<nes::calcite::PhysicalOperator::Field>& fields) const
    {
        return fields
            | std::views::transform(
                   [](const auto& field)
                   {
                       return QualifiedUnboundField(
                           fieldName(field.name()),
                           DataTypeProvider::provideDataType(
                               fieldType(field.type()), field.nullable() ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE));
                   })
            | std::ranges::to<PhysicalSchema>();
    }

    static DataType::Type fieldType(const nes::calcite::FieldType type)
    {
        switch (type)
        {
            case nes::calcite::INT8:
                return DataType::Type::INT8;
            case nes::calcite::INT16:
                return DataType::Type::INT16;
            case nes::calcite::INT32:
                return DataType::Type::INT32;
            case nes::calcite::INT64:
                return DataType::Type::INT64;
            case nes::calcite::UINT8:
                return DataType::Type::UINT8;
            case nes::calcite::UINT16:
                return DataType::Type::UINT16;
            case nes::calcite::UINT32:
                return DataType::Type::UINT32;
            case nes::calcite::UINT64:
            case nes::calcite::TIMESTAMP_MS:
                return DataType::Type::UINT64;
            case nes::calcite::FLOAT32:
                return DataType::Type::FLOAT32;
            case nes::calcite::FLOAT64:
                return DataType::Type::FLOAT64;
            case nes::calcite::BOOLEAN:
                return DataType::Type::BOOLEAN;
            case nes::calcite::CHAR:
                return DataType::Type::CHAR;
            case nes::calcite::VARSIZED:
                return DataType::Type::VARSIZED;
            default:
                throw UnsupportedQuery("unknown field type {}", static_cast<int>(type));
        }
    }

    /// The physical function and its type. Field references are positions in the operator's input.
    std::pair<PhysicalFunction, DataType> function(const substrait::Expression& expression, const PhysicalSchema& input) const
    {
        switch (expression.rex_type_case())
        {
            case substrait::Expression::kSelection: {
                const auto& selection = expression.selection();
                if (not selection.has_direct_reference() or not selection.direct_reference().has_struct_field()
                    or selection.direct_reference().struct_field().has_child())
                {
                    throw UnsupportedQuery("only plain field references are supported: {}", selection.ShortDebugString());
                }
                const auto& field = *std::ranges::next(input.begin(), selection.direct_reference().struct_field().field());
                return {FieldAccessPhysicalFunction(field.getFullyQualifiedName()), field.getDataType()};
            }
            case substrait::Expression::kLiteral:
                return literal(expression.literal());
            case substrait::Expression::kCast: {
                auto [child, childType] = function(expression.cast().input(), input);
                const auto type = detail::toDataType(expression.cast().type(), userDefinedTypes);
                return {registered("CastToType", {child}, {childType}, type), type};
            }
            case substrait::Expression::kScalarFunction:
                return scalarFunction(expression.scalar_function(), input);
            case substrait::Expression::kIfThen: {
                /// [condition1, result1, ..., default]; the type of a CASE is the type of its default.
                std::vector<PhysicalFunction> children;
                std::vector<DataType> types;
                const auto add = [&](const substrait::Expression& child)
                {
                    auto [childFunction, childType] = function(child, input);
                    children.push_back(std::move(childFunction));
                    types.push_back(childType);
                };
                for (const auto& clause : expression.if_then().ifs())
                {
                    add(clause.if_());
                    add(clause.then());
                }
                add(expression.if_then().else_());
                const auto type = types.back();
                return {registered("Conditional", children, types, type), type};
            }
            default:
                throw UnsupportedQuery("Substrait expression {} is not supported", expression.ShortDebugString());
        }
    }

    std::pair<PhysicalFunction, DataType> scalarFunction(const substrait::Expression::ScalarFunction& call, const PhysicalSchema& input) const
    {
        const auto found = functions.find(call.function_reference());
        if (found == functions.end())
        {
            throw UnsupportedQuery("Substrait function reference {} is not declared", call.function_reference());
        }
        std::vector<PhysicalFunction> children;
        std::vector<DataType> types;
        for (const auto& argument : call.arguments())
        {
            if (not argument.has_value())
            {
                throw UnsupportedQuery("function {} has a non-value argument", found->second);
            }
            auto [child, type] = function(argument.value(), input);
            children.push_back(std::move(child));
            types.push_back(type);
        }
        const auto type = detail::toDataType(call.output_type(), userDefinedTypes);
        static const std::unordered_map<std::string, std::string> names{
            {"add", "Add"},
            {"subtract", "Sub"},
            {"multiply", "Mul"},
            {"divide", "Div"},
            {"modulus", "Mod"},
            {"equal", "Equals"},
            {"lt", "Less"},
            {"lte", "LessEquals"},
            {"gt", "Greater"},
            {"gte", "GreaterEquals"},
            {"not", "Negate"},
            {"abs", "Abs"},
            {"is_null", "IsNull"}};
        const auto& name = found->second;
        if (const auto registeredName = names.find(name); registeredName != names.end())
        {
            return {registered(registeredName->second, children, types, type), type};
        }
        const auto boolean = DataTypeProvider::provideDataType(
            DataType::Type::BOOLEAN, type.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
        if ((name == "and" or name == "or") and not children.empty())
        {
            /// Binary, from the left.
            auto result = children.front();
            auto resultType = types.front();
            for (const auto& [child, childType] : std::views::zip(children, types) | std::views::drop(1))
            {
                result = registered(name == "and" ? "And" : "Or", {result, child}, {resultType, childType}, type);
                resultType = type;
            }
            return {result, type};
        }
        if (name == "not_equal" and children.size() == 2)
        {
            return {registered("Negate", {registered("Equals", children, types, type)}, {type}, type), type};
        }
        if (name == "is_not_null" and children.size() == 1)
        {
            return {registered("Negate", {registered("IsNull", children, types, boolean)}, {boolean}, type), type};
        }
        if (name == "negate" and children.size() == 1)
        {
            auto zero = constantFunction(ConstantValueLogicalFunction(types.front(), "0"));
            return {registered("Sub", {zero, children.front()}, {types.front(), types.front()}, type), type};
        }
        throw UnsupportedQuery("Substrait function {} is not supported", name);
    }

    /// Our lowering of constants; it does not read the field mapping it takes.
    static PhysicalFunction constantFunction(const ConstantValueLogicalFunction& constant)
    {
        return QueryCompilation::FunctionProvider::lowerFunction(constant, FieldMappingTrait({}));
    }

    static PhysicalFunction
    registered(const std::string& name, std::vector<PhysicalFunction> children, std::vector<DataType> types, const DataType& outputType)
    {
        const auto factory = PhysicalFunctionRegistry::instance().find(name);
        if (not factory)
        {
            throw UnsupportedQuery("no physical function {}", name);
        }
        return (*factory)(PhysicalFunctionRegistryArguments{.childFunctions = std::move(children), .inputTypes = std::move(types), .outputType = outputType});
    }

    std::pair<PhysicalFunction, DataType> literal(const substrait::Expression::Literal& literal) const
    {
        const auto constant = [&](const DataType::Type type, std::string value) -> std::pair<PhysicalFunction, DataType>
        {
            const auto dataType = DataTypeProvider::provideDataType(type, DataType::NULLABLE::NOT_NULLABLE);
            return {constantFunction(ConstantValueLogicalFunction(dataType, std::move(value))), dataType};
        };
        switch (literal.literal_type_case())
        {
            case substrait::Expression::Literal::kBoolean:
                return constant(DataType::Type::BOOLEAN, literal.boolean() ? "true" : "false");
            case substrait::Expression::Literal::kI8:
                return constant(DataType::Type::INT8, std::to_string(literal.i8()));
            case substrait::Expression::Literal::kI16:
                return constant(DataType::Type::INT16, std::to_string(literal.i16()));
            case substrait::Expression::Literal::kI32:
                return constant(DataType::Type::INT32, std::to_string(literal.i32()));
            case substrait::Expression::Literal::kI64:
                return constant(DataType::Type::INT64, std::to_string(literal.i64()));
            case substrait::Expression::Literal::kFp32:
                return constant(DataType::Type::FLOAT32, fmt::format("{}", literal.fp32()));
            case substrait::Expression::Literal::kFp64:
                return constant(DataType::Type::FLOAT64, fmt::format("{}", literal.fp64()));
            case substrait::Expression::Literal::kString:
                return constant(DataType::Type::VARSIZED, literal.string());
            case substrait::Expression::Literal::kVarChar:
                return constant(DataType::Type::VARSIZED, literal.var_char().value());
            case substrait::Expression::Literal::kFixedChar:
                return constant(DataType::Type::VARSIZED, literal.fixed_char());
            case substrait::Expression::Literal::kNull: {
                const auto type = detail::toDataType(literal.null(), userDefinedTypes);
                return {NullValuePhysicalFunction(type), type};
            }
            default:
                throw UnsupportedQuery("Substrait literal {} is not supported", literal.ShortDebugString());
        }
    }
};
}

std::shared_ptr<PipelinedQueryPlan> toPipelines(
    const substrait::Plan& plan,
    const PhysicalPlanContext& context,
    const Host& worker,
    const QueryId queryId,
    const QueryExecutionConfiguration& configuration)
{
    return Converter(plan, context, worker, queryId, configuration).convert(plan);
}

}
