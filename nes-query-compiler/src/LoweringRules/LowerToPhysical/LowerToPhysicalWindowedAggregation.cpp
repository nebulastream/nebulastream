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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalWindowedAggregation.hpp>

#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <bit>
#include <memory>
#include <numeric>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationProbePhysicalOperator.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <LoweringRules/LowerToPhysical/TraceSourcesOfInputOrigins.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <Functions/ConstantValuePhysicalFunction.hpp>
#include <Util/RowKind.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/SessionWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <magic_enum/magic_enum.hpp>

#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <WindowBasedOperatorHandler.hpp>
#include <Aggregation/SessionWindowOperatorHandler.hpp>
#include <Aggregation/SessionWindowProbePhysicalOperator.hpp>
#include <Aggregation/UnboundedAggregationOperatorHandler.hpp>
#include <Aggregation/UnboundedAggregationProbePhysicalOperator.hpp>
#include <Watermark/TimeFunction.hpp>

namespace NES
{
namespace
{
std::vector<std::shared_ptr<AggregationPhysicalFunction>>
getAggregationPhysicalFunctions(const WindowedAggregationLogicalOperator& logicalOperator, const QueryExecutionConfiguration& configuration)
{
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    const auto& aggregationDescriptors = logicalOperator.getWindowAggregation();

    const auto memoryLayoutTypeTrait = logicalOperator.getChild()->getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;
    const auto physicalInputSchema = createPhysicalOutputSchema(logicalOperator.getChild()->getTraitSet());
    auto tupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(physicalInputSchema);
    auto bufferRef = LowerSchemaProvider::lowerSchema(configuration.operatorBufferSize, physicalInputSchema, memoryLayoutType);

    for (const auto& descriptor : aggregationDescriptors)
    {
        const auto fieldIsBound
            = std::holds_alternative<TypedLogicalFunction<FieldAccessLogicalFunction>>(descriptor.function.getInputFunction());
        PRECONDITION(fieldIsBound, "Expected the aggregation function to be bound");
        const auto fieldAccessFunction = std::get<TypedLogicalFunction<FieldAccessLogicalFunction>>(descriptor.function.getInputFunction());

        auto physicalInputType = fieldAccessFunction->getDataType();
        auto physicalFinalType = descriptor.function->getAggregateType();
        auto aggregationInputFunction = QueryCompilation::FunctionProvider::lowerFunction(
            fieldAccessFunction, *logicalOperator.getChild().getTraitSet().get<FieldMappingTrait>());
        const auto resultFieldIdentifier = descriptor.name;
        auto name = descriptor.function->getName();

        auto aggregationArguments = AggregationPhysicalFunctionRegistryArguments(
            std::move(physicalInputType),
            std::move(physicalFinalType),
            std::move(aggregationInputFunction),
            resultFieldIdentifier,
            tupleLayout,
            descriptor.function.shallIncludeNullValues(),
            configuration.countDistinctNumberOfBuckets,
            configuration.operatorBufferSize);
        if (const auto aggregationFactory = AggregationPhysicalFunctionRegistry::instance().find(std::string{name}))
        {
            aggregationPhysicalFunctions.push_back((*aggregationFactory)(std::move(aggregationArguments)));
        }
        else
        {
            throw UnknownAggregationType("unknown aggregation type: {}", name);
        }
    }
    return aggregationPhysicalFunctions;
}
}

LoweringRuleResultSubgraph LowerToPhysicalWindowedAggregation::apply(LogicalOperator logicalOperator)
{
    auto aggregation = logicalOperator.getAs<WindowedAggregationLogicalOperator>();
    const auto traitSet = logicalOperator.getTraitSet();
    const auto childTraitSet = aggregation->getChild().getTraitSet();

    auto outputOriginIds = traitSet.get<OutputOriginIdsTrait>();
    PRECONDITION(std::ranges::size(*outputOriginIds) == 1, "Expected one output origin id");
    auto outputOriginId = (*outputOriginIds)[0];

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait->memoryLayout;

    PRECONDITION(
        std::holds_alternative<Windowing::BoundTimeCharacteristic>(aggregation->getCharacteristic()),
        "Expected time characteristic to be bound");

    auto handlerId = getNextOperatorHandlerId();
    auto timeFunction = TimeFunction::create(std::get<Windowing::BoundTimeCharacteristic>(aggregation->getCharacteristic()));
    auto windowType = aggregation->getWindowType();
    /// An unbounded window has no size: its tumbling slices of the configured size only batch the changelog the aggregation emits.
    /// Late records go to the earliest slice that has not been triggered, as they only change when their update is emitted.
    const bool unboundedWindow = windowType.isUnbounded();
    if (unboundedWindow)
    {
        timeFunction = std::make_unique<WatermarkClampedTimeFunction>(std::move(timeFunction));
    }
    /// A session window uses tumbling slices as wide as its gap: all records of a key within one slice are less than the gap apart
    /// and belong to the same session, so the slices pre-aggregate them like those of any other window.
    const auto sessionGap = windowType.isSession()
        ? std::optional{std::get<Windowing::SessionWindow>(windowType.getUnderlying()).getGap().getTime()}
        : std::nullopt;
    const auto sliceSize = [&]
    {
        if (unboundedWindow)
        {
            return conf.unboundedSliceSizeMs;
        }
        return sessionGap.has_value() ? *sessionGap : windowType.getSize().getTime();
    }();
    const auto sliceSlide = [&]
    {
        if (unboundedWindow)
        {
            return conf.unboundedSliceSizeMs;
        }
        return sessionGap.has_value() ? *sessionGap : windowType.getSlide().getTime();
    }();
    auto aggregationPhysicalFunctions = getAggregationPhysicalFunctions(*aggregation, conf);
    /// An unbounded aggregation counts the records of every key in a hidden COUNT, its last function: a key whose records were all
    /// withdrawn is absent and emits a DELETE.
    const Record::RecordFieldIdentifier rowCountField{Identifier::parse("$ROWCOUNT")};
    /// A session window counts the records of its open session in the same way, so a key with a count of zero has no open session.
    /// The hidden MIN and MAX of the records' timestamps, which the build writes into sessionTimestampField, follow the COUNT.
    const DataType hiddenType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    const Record::RecordFieldIdentifier sessionTimestampField{Identifier::parse("$SESSION_TS")};
    const Record::RecordFieldIdentifier sessionFirstField{Identifier::parse("$SESSION_FIRST")};
    const Record::RecordFieldIdentifier sessionLastField{Identifier::parse("$SESSION_LAST")};
    const auto addHiddenAggregation = [&](const std::string_view name, PhysicalFunction input, const Record::RecordFieldIdentifier& result)
    {
        const auto factory = AggregationPhysicalFunctionRegistry::instance().find(std::string{name});
        INVARIANT(factory.has_value(), "The {} aggregation is not registered", name);
        aggregationPhysicalFunctions.push_back(
            (*factory)(AggregationPhysicalFunctionRegistryArguments(hiddenType, hiddenType, std::move(input), result, std::nullopt, true)));
    };
    if (unboundedWindow or sessionGap.has_value())
    {
        addHiddenAggregation("Count", ConstantUInt64ValueFunction{1}, rowCountField);
    }
    if (sessionGap.has_value())
    {
        addHiddenAggregation("Min", FieldAccessPhysicalFunction{sessionTimestampField}, sessionFirstField);
        addHiddenAggregation("Max", FieldAccessPhysicalFunction{sessionTimestampField}, sessionLastField);
    }
    /// An updating input lifts every record with the weight of its row kind.
    const auto rowKindFunction = [&]() -> std::optional<PhysicalFunction>
    {
        const auto childRowKind = aggregation->getChild().getOutputSchema()[rowKindIdentifier()];
        if (not childRowKind.has_value())
        {
            return std::nullopt;
        }
        return QueryCompilation::FunctionProvider::lowerFunction(
            FieldAccessLogicalFunction{*childRowKind}, *aggregation->getChild().getTraitSet().get<FieldMappingTrait>());
    }();

    const auto physicalInputSchema = createPhysicalOutputSchema(childTraitSet);
    const auto physicalOutputSchema = createPhysicalOutputSchema(traitSet);

    const auto valueSize = std::accumulate(
        aggregationPhysicalFunctions.begin(),
        aggregationPhysicalFunctions.end(),
        0,
        [](const auto& sum, const auto& function) { return sum + function->getSizeOfStateInBytes(); });

    uint64_t keySize = 0;
    std::vector<PhysicalFunction> keyFunctions;
    auto boundGroupingKeys = aggregation->getGroupingKeys();
    for (auto& nodeFunctionKey : boundGroupingKeys)
    {
        auto loweredFunctionType = nodeFunctionKey.getDataType();
        keyFunctions.emplace_back(
            QueryCompilation::FunctionProvider::lowerFunction(
                nodeFunctionKey, *aggregation->getChild().getTraitSet().get<FieldMappingTrait>()));
        keySize += loweredFunctionType.getSizeInBytesWithNull();
    }
    const auto entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;
    /// The state is stored in buffers of the operator buffer size, so the chains of the hash map have to fit into one.
    const auto numberOfBuckets = ChainedHashMap::limitNumberOfBuckets(conf.numberOfPartitions, conf.operatorBufferSize);
    if (numberOfBuckets < conf.numberOfPartitions)
    {
        NES_WARNING(
            "Using {} instead of {} partitions for the aggregation, as the chains of the hash map have to fit into an operator buffer of "
            "{} bytes.",
            numberOfBuckets,
            conf.numberOfPartitions,
            conf.operatorBufferSize);
    }
    if (conf.operatorBufferSize < entrySize)
    {
        throw QueryCompilerError(
            "The aggregation hash map needs buffers of at least {} bytes for one entry, but the operator buffer size is {}. Increase "
            "operator_buffer_size.",
            entrySize,
            conf.operatorBufferSize);
    }

    const auto fieldKeyNames
        = boundGroupingKeys | std::views::transform([](const auto& field) { return QualifiedIdentifier{field->getField().getLastName()}; });
    const auto fieldValueNames
        = aggregation->getWindowAggregation()
        | std::views::transform(
              [](const auto& projectedAggregation) -> QualifiedIdentifier
              {
                  return std::get<TypedLogicalFunction<FieldAccessLogicalFunction>>(projectedAggregation.function->getInputFunction())
                      ->getField()
                      .getLastName();
              });
    const auto& [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(
        physicalInputSchema, fieldKeyNames | std::ranges::to<std::vector>(), fieldValueNames | std::ranges::to<std::vector>());

    const auto windowMetaData = WindowMetaData{aggregation->getWindowStartField(), aggregation->getWindowEndField()};

    /// Aggregation does not benefit of using the ChainedHashMap's optional filter, so bloomFilter stays empty.
    const ChainedHashMapConfig hashMapConfig{
        .entrySize = entrySize,
        .numberOfBuckets = numberOfBuckets,
        .bloomFilterParams = std::nullopt,
        .fieldKeys = fieldKeys,
        .fieldValues = fieldValues,
        .hashFunction = std::make_shared<MurMur3HashFunction>()};

    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(sliceSize, sliceSlide, conf.sliceCacheConfiguration);
    auto sliceStoreRef = sliceAndWindowStore->createSliceStoreRef(
        [](Slice& slice, const WorkerThreadId workerThreadId, AbstractBufferProvider& bufferProvider) -> const TupleBuffer*
        {
            auto& aggregationSlice = dynamic_cast<AggregationSlice&>(slice);
            return aggregationSlice.getOrCreateHashMapBufferRefForWorker(bufferProvider, workerThreadId);
        },
        /// NOLINTNEXTLINE(bugprone-exception-escape): dynamic_cast<ref> may throw std::bad_cast on bug; non-recoverable here.
        [hashMapConfig](WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
        {
            auto& aggHandler = dynamic_cast<AggregationOperatorHandler&>(handler);
            const CreateNewHashMapSliceArgs hashMapSliceArgs{hashMapConfig, bufferProvider};
            return handler.getCreateNewSlicesFunction(hashMapSliceArgs);
        });
    const AggregationBuildPhysicalOperator build{
        handlerId,
        std::move(timeFunction),
        std::move(sliceStoreRef),
        aggregationPhysicalFunctions,
        hashMapConfig,
        keyFunctions,
        rowKindFunction,
        /*writeArrivalOrder=*/false,
        sessionGap.has_value() ? std::optional{sessionTimestampField} : std::nullopt};
    auto sourcesOfInputOrigins = traceThrottleableSourcesOfInputOrigins(logicalOperator, windowsWithoutThrottling);
    const auto [probe, handler] = [&]() -> std::pair<PhysicalOperator, std::shared_ptr<AggregationOperatorHandler>>
    {
        if (sessionGap.has_value())
        {
            return {
                SessionWindowProbePhysicalOperator{
                    hashMapConfig,
                    aggregationPhysicalFunctions,
                    handlerId,
                    windowMetaData,
                    rowCountField,
                    sessionFirstField,
                    sessionLastField,
                    *sessionGap},
                std::make_shared<SessionWindowOperatorHandler>(
                    std::move(sourcesOfInputOrigins), outputOriginId, std::move(sliceAndWindowStore))};
        }
        if (unboundedWindow)
        {
            /// A partition is a range of the chains of the hash maps, so the number of partitions is a power of two that divides them.
            const auto numberOfChains = ChainedHashMap::calculateNumberOfChains(hashMapConfig.numberOfBuckets);
            const auto numberOfPartitions = std::min(std::bit_floor(conf.unboundedAggregationPartitions), numberOfChains);
            return {
                UnboundedAggregationProbePhysicalOperator{
                    hashMapConfig, aggregationPhysicalFunctions, handlerId, windowMetaData, rowCountField, numberOfPartitions},
                std::make_shared<UnboundedAggregationOperatorHandler>(
                    std::move(sourcesOfInputOrigins), outputOriginId, std::move(sliceAndWindowStore), numberOfPartitions)};
        }
        return {
            AggregationProbePhysicalOperator{hashMapConfig, aggregationPhysicalFunctions, handlerId, windowMetaData},
            std::make_shared<AggregationOperatorHandler>(std::move(sourcesOfInputOrigins), outputOriginId, std::move(sliceAndWindowStore))};
    }();
    auto buildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        build,
        physicalInputSchema,
        physicalOutputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        probe,
        physicalInputSchema,
        physicalOutputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{buildWrapper});

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leaves(logicalOperator.getChildren().size(), buildWrapper);
    return {.root = probeWrapper, .leaves = {leaves}};
}
}
