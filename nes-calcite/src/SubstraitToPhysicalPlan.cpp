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

#include <algorithm>
#include <cctype>
#include <Calcite/PhysicalParts.hpp>
#include <Util/Reflection.hpp>
#include <rfl/Generic.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_set>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/ConstantValuePhysicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/NullValuePhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
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
#include <WindowMetaData.hpp>
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
#include <Join/HashJoin/HJBuildPhysicalOperator.hpp>
#include <Join/HashJoin/HJInnerProbePhysicalOperator.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/HashJoin/HJOuterProbePhysicalOperator.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/JoinTriggerStrategy.hpp>
#include <Join/NestedLoopJoin/NLJBuildPhysicalOperator.hpp>
#include <Join/NestedLoopJoin/NLJInnerProbePhysicalOperator.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJOuterProbePhysicalOperator.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Functions/BooleanFunctions/AndPhysicalFunction.hpp>
#include <Join/JoinType.hpp>
#include <Interface/Hash/BloomFilterRef.hpp>
#include <InferModelPhysicalOperator.hpp>
#include <Inference.hpp>
#include <ModelCatalog.hpp>
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

/// What a worker's part reads from the coordinator's catalogs, resolved there (see resolveWorkerPart).
struct Resolved
{
    /// By the pipeline id of each SourceScan and NetworkSource.
    std::unordered_map<uint32_t, SourceDescriptor> sources;
    std::optional<SinkDescriptor> sink;
    /// By the model's name in the plan.
    std::unordered_map<std::string, RegisteredModel> models;
};

template <typename T>
std::string toJson(const T& value)
{
    return rfl::json::write(*ReflectionContext{}.reflect(value));
}

template <typename T>
T fromJson(const std::string& json)
{
    auto parsed = rfl::json::read<rfl::Generic>(json);
    if (not parsed.has_value())
    {
        throw CannotDeserialize("a part of a physical plan: {}", parsed.error().what());
    }
    return ReflectionContext{}.unreflect<T>(*parsed);
}

/// The pipelines of a physical plan, built as its payloads describe them.
class Converter
{
    friend Schema<UnqualifiedUnboundField, Ordered> NES::Calcite::sinkInputSchema(const substrait::Plan& plan);

public:
    Converter(
        const substrait::Plan& plan,
        Resolved resolved,
        Host worker,
        const QueryId queryId,
        const QueryExecutionConfiguration& configuration)
        : resolved(std::move(resolved))
        , worker(std::move(worker))
        , bufferSize(configuration.operatorBufferSize)
        , numberOfPartitions(configuration.numberOfPartitions)
        , pageSize(configuration.pageSize)
        , sliceCacheConfiguration(configuration.sliceCacheConfiguration)
        , bloomFilterParams(
              configuration.bloomFilterConfiguration.enableBloomFilter
                  ? std::optional{Nautilus::Interface::BloomFilterParams{
                        configuration.bloomFilterConfiguration.expectedEntries, configuration.bloomFilterConfiguration.falsePositiveRate}}
                  : std::nullopt)
        , functions(detail::functionNames(plan))
        , userDefinedTypes(detail::unsignedTypes(plan))
        , result(std::make_shared<PipelinedQueryPlan>(
              queryId, ExecutionConfiguration().withExecutionMode(configuration.executionMode).withNautilusInlining(configuration.nautilusInlining)))
    {
    }

    /// The part of the root `relationIndex` for `worker`, resolved from the coordinator's catalogs: what convert() reads
    /// besides the payloads (the descriptors of its sources and sink, and its models).
    static nes::calcite::WorkerPart resolve(
        const substrait::Plan& plan, const int relationIndex, const PhysicalPlanContext& context, const Host& worker)
    {
        nes::calcite::WorkerPart part;
        part.set_host(worker.getRawValue());
        substrait::Plan own = plan;
        own.clear_relations();
        *own.add_relations() = plan.relations(relationIndex);
        part.set_plan(own.SerializeAsString());

        const auto& root = plan.relations(relationIndex).root().input();
        const auto last = payload(root).first;
        const auto outputSchema = unqualified(schema(last.output_schema()));
        if (last.has_sink())
        {
            part.set_sink(toJson(context.bindSink(outputSchema)));
        }
        else if (last.has_network_sink())
        {
            const auto sink = context.sinkCatalog.getAnonymousSink(
                outputSchema,
                Identifier::parse("Network"),
                worker,
                {{Identifier::parse("channel"), last.network_sink().channel()},
                 {Identifier::parse("bind"), dataAddress(context, worker)},
                 {Identifier::parse("data_endpoint"), dataAddress(context, Host(last.network_sink().downstream_host()))},
                 {Identifier::parse("output_format"), "NATIVE"}},
                {});
            if (not sink)
            {
                throw UnsupportedQuery("cannot create the network sink of channel {}", last.network_sink().channel());
            }
            part.set_sink(toJson(*sink));
        }
        else
        {
            throw UnsupportedQuery("a part of a physical plan ends in a sink, not in {}", last.ShortDebugString());
        }

        std::vector<const substrait::Rel*> pending{&root};
        while (not pending.empty())
        {
            const auto [physical, inputs] = payload(*pending.back());
            pending.pop_back();
            pending.insert(pending.end(), inputs.begin(), inputs.end());
            if (physical.has_source_scan())
            {
                const auto& scan = physical.source_scan();
                const auto descriptor = scan.has_anonymous()
                    ? anonymousSource(scan.anonymous(), context)
                    : context.sourceCatalog.getPhysicalSource(PhysicalSourceId(scan.physical_source_id()));
                if (not descriptor)
                {
                    throw UnsupportedQuery("unknown physical source {}", scan.physical_source_id());
                }
                (*part.mutable_sources())[physical.pipeline()] = toJson(*descriptor);
            }
            else if (physical.has_network_source())
            {
                const auto& network = physical.network_source();
                const auto descriptor = context.sourceCatalog.getAnonymousSource(
                    Identifier::parse("Network"),
                    unqualified(schema(physical.output_schema())),
                    worker,
                    {{InputFormatterDescriptor::TYPE_FIELD.getName(), "NATIVE"}},
                    {{Identifier::parse("channel"), network.channel()}, {Identifier::parse("bind"), dataAddress(context, worker)}});
                if (not descriptor)
                {
                    throw UnsupportedQuery("cannot create the network source of channel {}", network.channel());
                }
                (*part.mutable_sources())[physical.pipeline()] = toJson(*descriptor);
            }
            else if (physical.has_infer_model())
            {
                const auto& model = physical.infer_model().model();
                if (context.models == nullptr)
                {
                    throw UnsupportedQuery("model inference needs the model catalog");
                }
                const auto name = context.catalogNames.contains(model) ? context.catalogNames.at(model) : model;
                (*part.mutable_models())[model] = toJson(context.models->load(name));
            }
        }
        return part;
    }

    /// What resolve() wrote into the part.
    static Resolved fromPart(const nes::calcite::WorkerPart& part)
    {
        Resolved resolved;
        for (const auto& [pipeline, descriptor] : part.sources())
        {
            resolved.sources.emplace(pipeline, fromJson<SourceDescriptor>(descriptor));
        }
        if (not part.sink().empty())
        {
            resolved.sink = fromJson<SinkDescriptor>(part.sink());
        }
        for (const auto& [name, model] : part.models())
        {
            resolved.models.emplace(name, fromJson<RegisteredModel>(model));
        }
        return resolved;
    }

    /// One part of the plan (a Substrait root) that runs on this worker, as one plan with one sink.
    std::shared_ptr<PipelinedQueryPlan> convert(const substrait::Plan& plan, const int relationIndex)
    {
        for (const auto& relation : std::views::single(plan.relations(relationIndex)))
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
            sinkColumns = outputSchema
                | std::views::transform([](const auto& field) { return fmt::format("{}", field.getFullyQualifiedName()); })
                | std::ranges::to<std::vector>();
            sinkDescriptor = resolved.sink;
            if (not sinkDescriptor)
            {
                throw UnsupportedQuery("a part of a physical plan without its sink descriptor");
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

    Resolved resolved;
    Host worker;
    uint64_t bufferSize;
    uint64_t numberOfPartitions;
    uint64_t pageSize;
    SliceCacheConfiguration sliceCacheConfiguration;
    /// The hash joins' in-map bloom filter, or nullopt when it is switched off (as our lowering sizes it).
    std::optional<Nautilus::Interface::BloomFilterParams> bloomFilterParams;
    std::unordered_map<uint32_t, std::string> functions;
    detail::UserDefinedTypes userDefinedTypes;
    std::shared_ptr<PipelinedQueryPlan> result;
    std::optional<SinkDescriptor> sinkDescriptor;
    /// The names of the columns the sink receives, in the plan's order (its Emit writes the same columns by position).
    std::vector<std::string> sinkColumns;
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
            const auto& name = aggregation.function();
            /// The registry's names are the planner's (case-insensitive).
            const auto factory = AggregationPhysicalFunctionRegistry::instance().find(name);
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

    /// A window join's state, which the builds of both sides fill before its probe creates the handler that owns it. As our
    /// lowering (LowerToPhysicalHashJoin, LowerToPhysicalNLJoin) sets it up, from the stored rows of both sides.
    struct Join
    {
        OperatorHandlerId handlerId;
        std::unique_ptr<DefaultTimeBasedSliceStore> store;
        std::vector<uint32_t> buildPipelines;
        bool hash;
        JoinType type;
        PhysicalSchema left;
        PhysicalSchema right;
        std::shared_ptr<DefaultPagedVectorTupleLayout> leftLayout;
        std::shared_ptr<DefaultPagedVectorTupleLayout> rightLayout;
        std::vector<QualifiedIdentifier> leftKeys;
        std::vector<QualifiedIdentifier> rightKeys;
        std::optional<ChainedHashMapConfig> leftConfig;
        std::optional<ChainedHashMapConfig> rightConfig;
    };
    std::unordered_map<uint32_t, Join> joins;

    static JoinType joinType(const nes::calcite::WindowJoin::Type type, const bool hasCondition)
    {
        switch (type)
        {
            case nes::calcite::WindowJoin::LEFT:
                return JoinType::OUTER_LEFT_JOIN;
            case nes::calcite::WindowJoin::RIGHT:
                return JoinType::OUTER_RIGHT_JOIN;
            case nes::calcite::WindowJoin::FULL:
                return JoinType::OUTER_FULL_JOIN;
            default:
                return hasCondition ? JoinType::INNER_JOIN : JoinType::CARTESIAN_PRODUCT;
        }
    }

    Join& join(const nes::calcite::PhysicalOperator::Join& spec)
    {
        if (const auto found = joins.find(spec.handler()); found != joins.end())
        {
            return found->second;
        }
        Join state{
            .handlerId = getNextOperatorHandlerId(),
            .store = std::make_unique<DefaultTimeBasedSliceStore>(spec.size_ms(), spec.slide_ms(), sliceCacheConfiguration),
            .buildPipelines = {},
            .hash = spec.implementation() == nes::calcite::WindowJoin::HASH_JOIN,
            .type = joinType(spec.type(), true),
            .left = schema(spec.left_schema()),
            .right = schema(spec.right_schema()),
            .leftLayout = nullptr,
            .rightLayout = nullptr,
            .leftKeys = {},
            .rightKeys = {},
            .leftConfig = std::nullopt,
            .rightConfig = std::nullopt};
        state.leftLayout = std::make_shared<DefaultPagedVectorTupleLayout>(state.left);
        state.rightLayout = std::make_shared<DefaultPagedVectorTupleLayout>(state.right);
        for (const auto key : spec.left_key_fields())
        {
            state.leftKeys.push_back(fieldAt(state.left, key).getFullyQualifiedName());
        }
        for (const auto key : spec.right_key_fields())
        {
            state.rightKeys.push_back(fieldAt(state.right, key).getFullyQualifiedName());
        }
        if (state.hash)
        {
            state.leftConfig = joinHashMapConfig(state.left, state.leftKeys);
            state.rightConfig = joinHashMapConfig(state.right, state.rightKeys);
            /// As our lowering: a key's tuples live on pages of the configured page size, so a page must hold a tuple.
            for (const auto& [layout, side] : {std::pair{state.leftLayout, "left"}, std::pair{state.rightLayout, "right"}})
            {
                if (const auto minimumPageSize = layout->getMinimumPageSize(); pageSize < minimumPageSize)
                {
                    throw QueryCompilerError(
                        "The {} join input needs a page size of at least {} bytes, but the configured page size is {}. Increase the page "
                        "size.",
                        side,
                        minimumPageSize,
                        pageSize);
                }
            }
        }
        return joins.emplace(spec.handler(), std::move(state)).first->second;
    }

    /// The hash map of a hash join's side: an entry per key, whose rows live in a paged vector (as in our lowering).
    ChainedHashMapConfig joinHashMapConfig(const PhysicalSchema& stored, const std::vector<QualifiedIdentifier>& keys) const
    {
        uint64_t keySize = 0;
        for (const auto& key : keys)
        {
            keySize += stored[key]->getDataType().getSizeInBytesWithNull();
        }
        auto [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(stored, keys, {});
        return ChainedHashMapConfig{
            .entrySize = sizeof(ChainedHashMapEntry) + keySize + sizeof(uint32_t),
            .numberOfBuckets = numberOfPartitions,
            .pageSize = pageSize,
            .bloomFilterParams = bloomFilterParams,
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
                auto descriptor = std::optional{source(id)};
                result->addPipeline(open(id, std::make_shared<Pipeline>(SourceDescriptorPhysicalOperator(*descriptor, OriginId(scan.origin_id())))));
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::move(descriptor)};
            }
            case nes::calcite::PhysicalOperator::kScan: {
                const auto fields = names(outputSchema);
                /// As our PipeliningPhase checks it: a pipeline's buffers must hold at least one of its input rows.
                const auto& scanned = physical.scan().parses_source() ? inputs.at(0).source->getSchema() : outputSchema;
                if (getSizeInBytes(scanned) > bufferSize)
                {
                    throw TuplesTooLargeForPipelineBufferSize(
                        "Got pipeline with an input schema size of {}, which is larger than the configured buffer size of the pipeline, "
                        "which is {}",
                        getSizeInBytes(scanned),
                        bufferSize);
                }
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
                std::vector<std::pair<Record::RecordFieldIdentifier, PhysicalFunction>> assignments;
                for (const auto& assignment : physical.map().assignments())
                {
                    assignments.emplace_back(fieldName(assignment.field()), function(assignment.expression(), input.outputSchema).first);
                }
                pipelines.at(id)->appendOperator(MapPhysicalOperator(std::move(assignments)));
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::nullopt};
            }
            case nes::calcite::PhysicalOperator::kEmit: {
                /// A native sink (e.g. a NetworkSink) takes rows as they are, as in the PipeliningPhase.
                const auto bufferRef = physical.emit().for_sink() and not sinkDescriptor->getOutputFormatterDescriptor().isNative()
                    ? LowerSchemaProvider::lowerSchemaWithOutputFormat(
                          bufferSize, inSinkOrder(outputSchema), sinkDescriptor->getOutputFormatterDescriptor())
                    : LowerSchemaProvider::lowerSchema(bufferSize, outputSchema, ROW);
                const auto handlerId = getNextOperatorHandlerId();
                const auto& pipeline = pipelines.at(id);
                pipeline->getOperatorHandlers().emplace(handlerId, std::make_shared<EmitOperatorHandler>());
                pipeline->appendOperator(EmitPhysicalOperator(handlerId, bufferRef));
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::nullopt};
            }
            case nes::calcite::PhysicalOperator::kNetworkSource: {
                const auto& network = physical.network_source();
                auto descriptor = std::optional{source(id)};
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
                /// As our lowering does it for an aggregation without GROUP BY: the first window fires even without rows, and its
                /// row has counts of 0 and NULL for every other aggregation.
                std::optional<std::vector<EmptyWindowField>> recordOnEmptyWindow;
                if (spec.window().emit_on_empty_input())
                {
                    recordOnEmptyWindow.emplace();
                    for (int i = 0; i < spec.aggregations_size(); ++i)
                    {
                        const auto& output = *std::ranges::next(outputSchema.begin(), static_cast<std::ptrdiff_t>(2 + keys + i));
                        recordOnEmptyWindow->push_back(
                            {.field = output.getFullyQualifiedName(),
                             .type = output.getDataType(),
                             .isCount = spec.aggregations(i).function() == "COUNT"});
                    }
                    state.store->registerEmptyWindow(WindowInfo{0, spec.window().size_ms()});
                }
                open(id, std::make_shared<Pipeline>(PhysicalOperator(AggregationProbePhysicalOperator(
                             config, aggregations, state.handlerId, WindowMetaData{field(0), field(1)}, std::move(recordOnEmptyWindow)))));
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
            case nes::calcite::PhysicalOperator::kJoinBuild: {
                const auto& spec = physical.join_build();
                const auto& input = inputs.at(0).outputSchema;
                auto& state = join(spec.join());
                const auto& stored = spec.right() ? state.right : state.left;
                /// The side's rows under the names of its stored row (a branch below, e.g. of a UNION ALL, has its own).
                std::vector<std::pair<Record::RecordFieldIdentifier, PhysicalFunction>> copies;
                for (int i = 0; i < spec.stored_fields_size(); ++i)
                {
                    const auto& source = fieldAt(input, spec.stored_fields(i)).getFullyQualifiedName();
                    const auto& target = fieldAt(stored, i).getFullyQualifiedName();
                    if (source != target)
                    {
                        copies.emplace_back(target, FieldAccessPhysicalFunction(source));
                    }
                }
                if (not copies.empty())
                {
                    pipelines.at(id)->appendOperator(MapPhysicalOperator(std::move(copies)));
                }
                auto timeFunction = std::make_unique<EventTimeFunction>(
                    FieldAccessPhysicalFunction(fieldAt(input, spec.time_field()).getFullyQualifiedName()), Windowing::TimeUnit::Milliseconds());
                const auto side = spec.right() ? JoinBuildSideType::Right : JoinBuildSideType::Left;
                const auto& layout = spec.right() ? state.rightLayout : state.leftLayout;
                if (state.hash)
                {
                    const auto& config = spec.right() ? *state.rightConfig : *state.leftConfig;
                    std::vector<PhysicalFunction> keys;
                    for (const auto& key : spec.right() ? state.rightKeys : state.leftKeys)
                    {
                        keys.emplace_back(FieldAccessPhysicalFunction(key));
                    }
                    auto sliceStoreRef = state.store->createSliceStoreRef(
                        [side](Slice& slice, const WorkerThreadId workerThreadId, AbstractBufferProvider& bufferProvider)
                        { return dynamic_cast<HJSlice&>(slice).getOrCreateHashMapBufferRefForSide(workerThreadId, side, bufferProvider); },
                        [config](WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
                        { return handler.getCreateNewSlicesFunction(CreateNewHashMapSliceArgs{config, bufferProvider}); });
                    /// An outer join keeps the rows of its padded side whose key is NULL.
                    const bool keepNullKeys = spec.right()
                        ? state.type == JoinType::OUTER_RIGHT_JOIN or state.type == JoinType::OUTER_FULL_JOIN
                        : state.type == JoinType::OUTER_LEFT_JOIN or state.type == JoinType::OUTER_FULL_JOIN;
                    pipelines.at(id)->appendOperator(HJBuildPhysicalOperator(
                        state.handlerId, side, std::move(timeFunction), layout, config, std::move(keys), std::move(sliceStoreRef), keepNullKeys));
                }
                else
                {
                    const auto leftSize = getSizeInBytes(state.leftLayout->getSchema());
                    const auto rightSize = getSizeInBytes(state.rightLayout->getSchema());
                    auto sliceStoreRef = state.store->createSliceStoreRef(
                        [side](Slice& slice, const WorkerThreadId workerThreadId, AbstractBufferProvider&)
                        { return dynamic_cast<NLJSlice&>(slice).getPagedVectorTupleBufferRef(workerThreadId, side); },
                        [leftSize, rightSize](const WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
                        { return handler.getCreateNewSlicesFunction(CreateNewNLJSliceArgs{bufferProvider, leftSize, rightSize}); });
                    pipelines.at(id)->appendOperator(
                        NLJBuildPhysicalOperator(state.handlerId, side, std::move(timeFunction), layout, std::move(sliceStoreRef)));
                }
                state.buildPipelines.push_back(id);
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::nullopt};
            }
            case nes::calcite::PhysicalOperator::kJoinProbe: {
                const auto& spec = physical.join_probe();
                auto& state = join(spec.join());
                state.type = joinType(spec.join().type(), spec.has_condition());
                /// The condition and residual read [left stored row..., right stored row...].
                std::vector<QualifiedUnboundField> bothFields(state.left.begin(), state.left.end());
                bothFields.insert(bothFields.end(), state.right.begin(), state.right.end());
                const auto both = PhysicalSchema(std::move(bothFields));
                std::optional<PhysicalFunction> residual;
                for (const auto& expression : spec.residual())
                {
                    auto next = function(expression, both).first;
                    residual = residual ? PhysicalFunction{AndPhysicalFunction(*residual, std::move(next))} : std::move(next);
                }
                auto condition = spec.has_condition() ? function(spec.condition(), both).first : PhysicalFunction{ConstantBooleanValueFunction(true)};
                const auto window = WindowMetaData{
                    UnqualifiedUnboundField(*std::ranges::prev(fieldAt(outputSchema, 0).getFullyQualifiedName().end()), fieldAt(outputSchema, 0).getDataType()),
                    UnqualifiedUnboundField(*std::ranges::prev(fieldAt(outputSchema, 1).getFullyQualifiedName().end()), fieldAt(outputSchema, 1).getDataType())};
                const auto joinSchema = JoinSchema(state.left, state.right, outputSchema);
                const bool outer = isOuterJoin(state.type);
                auto probe = [&]() -> PhysicalOperator
                {
                    if (state.hash)
                    {
                        if (outer)
                        {
                            return HJOuterProbePhysicalOperator(
                                state.handlerId, condition, window, joinSchema, state.leftLayout, state.rightLayout, *state.leftConfig,
                                *state.rightConfig, residual);
                        }
                        return HJInnerProbePhysicalOperator(
                            state.handlerId, condition, window, joinSchema, state.leftLayout, state.rightLayout, *state.leftConfig,
                            *state.rightConfig, residual);
                    }
                    /// Our nested-loop join takes the residual as part of its condition.
                    if (residual)
                    {
                        condition = AndPhysicalFunction(condition, *residual);
                    }
                    if (outer)
                    {
                        return NLJOuterProbePhysicalOperator(
                            state.handlerId, condition, window, joinSchema, state.leftLayout, state.rightLayout, state.leftKeys, state.rightKeys);
                    }
                    return NLJInnerProbePhysicalOperator(
                        state.handlerId, condition, window, joinSchema, state.leftLayout, state.rightLayout, state.leftKeys, state.rightKeys);
                }();
                open(id, std::make_shared<Pipeline>(probe));
                const auto trigger = [&]() -> JoinTriggerStrategy
                {
                    switch (state.type)
                    {
                        case JoinType::OUTER_LEFT_JOIN:
                            return OuterJoinTriggerStrategy<true, false>{};
                        case JoinType::OUTER_RIGHT_JOIN:
                            return OuterJoinTriggerStrategy<false, true>{};
                        case JoinType::OUTER_FULL_JOIN:
                            return OuterJoinTriggerStrategy<true, true>{};
                        default:
                            return InnerJoinTriggerStrategy{};
                    }
                }();
                const auto origins = spec.input_origin_ids() | std::views::transform([](const auto origin) { return OriginId(origin); })
                    | std::ranges::to<std::vector>();
                std::shared_ptr<OperatorHandler> handler = state.hash
                    ? std::shared_ptr<OperatorHandler>(
                          std::make_shared<HJOperatorHandler>(origins, OriginId(spec.output_origin_id()), std::move(state.store), trigger))
                    : std::shared_ptr<OperatorHandler>(
                          std::make_shared<NLJOperatorHandler>(origins, OriginId(spec.output_origin_id()), std::move(state.store), trigger));
                for (const auto pipeline : state.buildPipelines)
                {
                    pipelines.at(pipeline)->getOperatorHandlers().emplace(state.handlerId, handler);
                }
                pipelines.at(id)->getOperatorHandlers().emplace(state.handlerId, handler);
                link(inputs, id);
                return {.pipeline = id, .outputSchema = std::move(outputSchema), .source = std::nullopt};
            }
            case nes::calcite::PhysicalOperator::kInferModel: {
                /// As our lowering (LowerToPhysicalInferModel): the worker compiles the imported model; the operator reads the
                /// model's inputs from the given fields and appends its outputs (the last fields of the output schema).
                const auto& spec = physical.infer_model();
                const auto found = resolved.models.find(spec.model());
                if (found == resolved.models.end())
                {
                    throw UnsupportedQuery("a part of a physical plan without the model {}", spec.model());
                }
                const auto& name = spec.model();
                const auto& model = found->second;
                auto compiled = compileModel(model.getImported());
                if (not compiled)
                {
                    throw CannotLoadModel("Failed to compile model {}: {}", name, compiled.error().message);
                }
                const auto& input = inputs.at(0).outputSchema;
                std::vector<QualifiedIdentifier> inputFields;
                for (const auto field : spec.input_fields())
                {
                    inputFields.push_back(fieldAt(input, field).getFullyQualifiedName());
                }
                const auto allFields = names(outputSchema);
                const std::vector<QualifiedIdentifier> outputFields{
                    allFields.begin() + static_cast<std::ptrdiff_t>(std::ranges::size(input)), allFields.end()};
                /// VARSIZED is the bulk-byte form: a VARSIZED side has exactly one field (ModelCatalog checks it).
                const auto& modelInputs = model.getSchema().inputs;
                const auto& modelOutputs = model.getSchema().outputs;
                const bool varsizedInput = modelInputs.size() > 0 and modelInputs.begin()->getDataType().isType(DataType::Type::VARSIZED);
                const bool varsizedOutput = modelOutputs.size() > 0 and modelOutputs.begin()->getDataType().isType(DataType::Type::VARSIZED);
                pipelines.at(id)->appendOperator(InferModelPhysicalOperator(
                    std::move(*compiled), std::move(inputFields), outputFields, varsizedInput, varsizedOutput));
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

    /// The source of the SourceScan or NetworkSource that opens the pipeline, as resolved.
    const SourceDescriptor& source(const uint32_t pipeline) const
    {
        const auto found = resolved.sources.find(pipeline);
        if (found == resolved.sources.end())
        {
            throw UnsupportedQuery("a part of a physical plan without the source of pipeline {}", pipeline);
        }
        return found->second;
    }

    /// The descriptor of an anonymous source in FROM, as our AnonymousSourceBindingRule creates it: its HOST option places
    /// it (the planner did), the other options configure it.
    static std::optional<SourceDescriptor> anonymousSource(const nes::calcite::AnonymousSource& spec, const PhysicalPlanContext& context)
    {
        const auto schema = spec.schema()
            | std::views::transform(
                                [](const auto& field)
                                {
                                    return UnqualifiedUnboundField(
                                        *std::ranges::prev(fieldName(field.name()).end()),
                                        DataTypeProvider::provideDataType(
                                            fieldType(field.type()),
                                            field.nullable() ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE));
                                })
            | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();
        std::unordered_map<Identifier, std::string> sourceOptions;
        for (const auto& [key, value] : spec.source_options())
        {
            sourceOptions.emplace(Identifier::parse(key), value);
        }
        const auto host = sourceOptions.find(Identifier::parse("host"));
        if (host == sourceOptions.end())
        {
            throw InvalidConfigParameter("`host`");
        }
        const auto worker = Host(host->second);
        sourceOptions.erase(host);
        std::unordered_map<Identifier, std::string> formatOptions;
        for (const auto& [key, value] : spec.format_options())
        {
            formatOptions.emplace(Identifier::parse(key), value);
        }
        auto descriptor = context.sourceCatalog.getAnonymousSource(Identifier::parse(spec.type()), schema, worker, formatOptions, sourceOptions);
        if (not descriptor)
        {
            throw InvalidConfigParameter("Could not create an anonymous source descriptor because of invalid config parameters");
        }
        return descriptor;
    }

    /// The Emit's columns in the order of the sink's schema, as our sink writes them: a named sink's declared order may
    /// differ from the query's (they must have the same columns, see planPhysical). By position, the Emit's columns are
    /// the sink's (sinkColumns).
    PhysicalSchema inSinkOrder(const PhysicalSchema& emitted) const
    {
        const auto sinkSchema = sinkDescriptor->getSchema();
        const auto* declared = std::get_if<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(&sinkSchema);
        if (declared == nullptr or *declared == nullptr or (*declared)->size() != emitted.size())
        {
            return emitted;
        }
        std::vector<QualifiedUnboundField> ordered;
        for (const auto& field : **declared)
        {
            const auto position = std::ranges::find(sinkColumns, fmt::format("{}", field.getFullyQualifiedName()));
            if (position == sinkColumns.end())
            {
                return emitted;
            }
            ordered.push_back(*std::ranges::next(emitted.begin(), std::ranges::distance(sinkColumns.begin(), position)));
        }
        return PhysicalSchema(std::move(ordered));
    }

    /// A field name from the planner. As in the logical converter (toIdentifier): Calcite returns quoted names as written, so a
    /// name with a lower-case letter is a quoted one (a field created as "id"), except Calcite's own generated names. A name
    /// in our syntax already (qualified or quoted, e.g. a physical source's field) is taken as it is.
    static QualifiedIdentifier fieldName(const std::string& name)
    {
        if (name.find_first_of(".\"") != std::string::npos)
        {
            return QualifiedIdentifier::parse(name);
        }
        const bool lowerCase = std::ranges::any_of(name, [](const unsigned char c) { return std::islower(c) != 0; });
        const bool generated = name == "window_start" or name == "window_end" or name.starts_with("$f");
        if (lowerCase and not generated)
        {
            return QualifiedIdentifier::parse(fmt::format("\"{}\"", name));
        }
        if (auto parsed = QualifiedIdentifier::tryParse(name))
        {
            return std::move(*parsed);
        }
        return QualifiedIdentifier::parse(fmt::format("\"{}\"", name));
    }

    static Schema<UnqualifiedUnboundField, Ordered> unqualified(const PhysicalSchema& schema)
    {
        return schema
            | std::views::transform([](const auto& field)
                                    { return UnqualifiedUnboundField(*std::ranges::prev(field.getFullyQualifiedName().end()), field.getDataType()); })
            | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();
    }

    static const std::string& dataAddress(const PhysicalPlanContext& context, const Host& host)
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

    static PhysicalSchema schema(const google::protobuf::RepeatedPtrField<nes::calcite::PhysicalOperator::Field>& fields)
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
                return {registered("cast", {child}, {childType}, {std::nullopt}, type), type};
            }
            case substrait::Expression::kScalarFunction:
                return scalarFunction(expression.scalar_function(), input);
            case substrait::Expression::kIfThen: {
                /// [condition1, result1, ..., default]; the type of a CASE is the type of its default, nullable if any result is.
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
                auto type = types.back();
                for (size_t result = 1; result < types.size(); result += 2)
                {
                    type.nullable = type.nullable or types.at(result).nullable;
                }
                return {registered("case", children, types, std::vector<std::optional<std::string>>(children.size()), type), type};
            }
            default:
                throw UnsupportedQuery("Substrait expression {} is not supported", expression.ShortDebugString());
        }
    }

    /// A call of the registry's function of the same name (its Substrait name). An argument that is a literal or an enum
    /// (e.g. the unit of EXTRACT) also gives the function its text.
    std::pair<PhysicalFunction, DataType> scalarFunction(const substrait::Expression::ScalarFunction& call, const PhysicalSchema& input) const
    {
        const auto found = functions.find(call.function_reference());
        if (found == functions.end())
        {
            throw UnsupportedQuery("Substrait function reference {} is not declared", call.function_reference());
        }
        std::vector<PhysicalFunction> children;
        std::vector<DataType> types;
        std::vector<std::optional<std::string>> literals;
        for (const auto& argument : call.arguments())
        {
            if (argument.has_enum_())
            {
                auto [child, type] = literal(textLiteral(argument.enum_()));
                children.push_back(std::move(child));
                types.push_back(type);
                literals.emplace_back(argument.enum_());
                continue;
            }
            if (not argument.has_value())
            {
                throw UnsupportedQuery("function {} has an argument that is not a value", found->second);
            }
            auto [child, type] = function(argument.value(), input);
            children.push_back(std::move(child));
            types.push_back(type);
            literals.push_back(literalText(argument.value()));
        }
        const auto type = detail::toDataType(call.output_type(), userDefinedTypes);
        return {registered(found->second, std::move(children), std::move(types), std::move(literals), type), type};
    }

    static substrait::Expression::Literal textLiteral(const std::string& text)
    {
        substrait::Expression::Literal literal;
        literal.set_string(text);
        return literal;
    }

    /// The text of a literal argument (of a text or a number), or nothing.
    static std::optional<std::string> literalText(const substrait::Expression& expression)
    {
        if (not expression.has_literal())
        {
            return std::nullopt;
        }
        const auto& literal = expression.literal();
        switch (literal.literal_type_case())
        {
            case substrait::Expression::Literal::kString:
                return literal.string();
            case substrait::Expression::Literal::kVarChar:
                return literal.var_char().value();
            case substrait::Expression::Literal::kFixedChar:
                return literal.fixed_char();
            case substrait::Expression::Literal::kI8:
                return std::to_string(literal.i8());
            case substrait::Expression::Literal::kI16:
                return std::to_string(literal.i16());
            case substrait::Expression::Literal::kI32:
                return std::to_string(literal.i32());
            case substrait::Expression::Literal::kI64:
                return std::to_string(literal.i64());
            default:
                return std::nullopt;
        }
    }

    /// Our lowering of constants.
    static PhysicalFunction constantFunction(const DataType& type, const std::string& value)
    {
        return QueryCompilation::FunctionProvider::lowerConstantFunction(type, value);
    }

    static PhysicalFunction registered(
        const std::string& name,
        std::vector<PhysicalFunction> children,
        std::vector<DataType> types,
        std::vector<std::optional<std::string>> literals,
        const DataType& outputType)
    {
        const auto factory = PhysicalFunctionRegistry::instance().find(name);
        if (not factory)
        {
            throw UnsupportedQuery("Substrait function {} is not supported", name);
        }
        return (*factory)(PhysicalFunctionRegistryArguments{
            .childFunctions = std::move(children), .inputTypes = std::move(types), .outputType = outputType, .literals = std::move(literals)});
    }

    std::pair<PhysicalFunction, DataType> literal(const substrait::Expression::Literal& literal) const
    {
        const auto constant = [&](const DataType::Type type, std::string value) -> std::pair<PhysicalFunction, DataType>
        {
            const auto dataType = DataTypeProvider::provideDataType(type, DataType::NULLABLE::NOT_NULLABLE);
            return {constantFunction(dataType, value), dataType};
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

Schema<UnqualifiedUnboundField, Ordered> sinkInputSchema(const substrait::Plan& plan)
{
    for (const auto& relation : plan.relations())
    {
        if (const auto last = Converter::payload(relation.root().input()).first; last.has_sink())
        {
            return Converter::unqualified(Converter::schema(last.output_schema()));
        }
    }
    throw UnsupportedQuery("a physical plan without a sink");
}

std::string resolveWorkerPart(const substrait::Plan& plan, const int relation, const PhysicalPlanContext& context, const Host& worker)
{
    return Converter::resolve(plan, relation, context, worker).SerializeAsString();
}

std::shared_ptr<PipelinedQueryPlan> toPipelines(const std::string_view workerPart, const QueryId queryId, const QueryExecutionConfiguration& configuration)
{
    nes::calcite::WorkerPart part;
    substrait::Plan plan;
    if (not part.ParseFromArray(workerPart.data(), static_cast<int>(workerPart.size())) or not plan.ParseFromString(part.plan()))
    {
        throw CannotDeserialize("a part of a physical plan");
    }
    return Converter(plan, Converter::fromPart(part), Host(part.host()), queryId, configuration).convert(plan, 0);
}

}
