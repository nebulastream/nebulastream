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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalWindowedAggregation.hpp>

#include <cstdint>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>
#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/SerializableAggregationOperatorHandler.hpp>
#include <Aggregation/AggregationProbePhysicalOperator.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/OffsetHashMap/OffsetEntryMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <magic_enum/magic_enum.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <HashMapOptions.hpp>
#include <PhysicalOperator.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES
{

static std::pair<std::vector<Record::RecordFieldIdentifier>, std::vector<Record::RecordFieldIdentifier>>
getKeyAndValueFields(const WindowedAggregationLogicalOperator& logicalOperator)
{
    std::vector<Record::RecordFieldIdentifier> fieldKeyNames;
    std::vector<Record::RecordFieldIdentifier> fieldValueNames;

    /// Getting the key  field names
    for (const auto& nodeAccess : logicalOperator.getGroupingKeys())
    {
        fieldKeyNames.emplace_back(nodeAccess.getFieldName());
    }
    /// For aggregation, we return empty value fields to trigger fallback logic
    /// The value area contains aggregation states, not input field values
    return {fieldKeyNames, fieldValueNames};
}

static std::unique_ptr<TimeFunction> getTimeFunction(const WindowedAggregationLogicalOperator& logicalOperator)
{
    auto* const timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(logicalOperator.getWindowType().get());
    if (timeWindow == nullptr)
    {
        throw UnknownWindowType("Window type is not a time based window type");
    }

    switch (timeWindow->getTimeCharacteristic().getType())
    {
        case Windowing::TimeCharacteristic::Type::IngestionTime: {
            if (timeWindow->getTimeCharacteristic().field.name == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME)
            {
                return std::make_unique<IngestionTimeFunction>();
            }
            throw UnknownWindowType(
                "The ingestion time field of a window must be: {}", Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME);
        }
        case Windowing::TimeCharacteristic::Type::EventTime: {
            /// For event time fields, we look up the reference field name and create an expression to read the field.
            auto timeCharacteristicField = timeWindow->getTimeCharacteristic().field.name;
            auto timeStampField = FieldAccessPhysicalFunction(timeCharacteristicField);
            return std::make_unique<EventTimeFunction>(timeStampField, timeWindow->getTimeCharacteristic().getTimeUnit());
        }
        default: {
            throw UnknownWindowType("Unknown window type: {}", magic_enum::enum_name(timeWindow->getTimeCharacteristic().getType()));
        }
    }
}

namespace
{
std::vector<std::shared_ptr<AggregationPhysicalFunction>>
getAggregationPhysicalFunctions(const WindowedAggregationLogicalOperator& logicalOperator, const QueryExecutionConfiguration& configuration)
{
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    const auto& aggregationDescriptors = logicalOperator.getWindowAggregation();
    for (const auto& descriptor : aggregationDescriptors)
    {
        auto physicalInputType = DataTypeProvider::provideDataType(descriptor->getInputStamp().type);
        auto physicalFinalType = DataTypeProvider::provideDataType(descriptor->getFinalAggregateStamp().type);

        auto aggregationInputFunction = QueryCompilation::FunctionProvider::lowerFunction(descriptor->onField);
        const auto resultFieldIdentifier = descriptor->asField.getFieldName();
        auto layout = std::make_shared<Memory::MemoryLayouts::ColumnLayout>(
            configuration.pageSize.getValue(), logicalOperator.getInputSchemas()[0]);
        auto memoryProvider = std::make_shared<Interface::MemoryProvider::ColumnTupleBufferMemoryProvider>(layout);

        auto name = descriptor->getName();
        auto aggregationArguments = AggregationPhysicalFunctionRegistryArguments(
            std::move(physicalInputType),
            std::move(physicalFinalType),
            std::move(aggregationInputFunction),
            resultFieldIdentifier,
            memoryProvider);
        if (auto aggregationPhysicalFunction
            = AggregationPhysicalFunctionRegistry::instance().create(std::string(name), std::move(aggregationArguments)))
        {
            aggregationPhysicalFunctions.push_back(aggregationPhysicalFunction.value());
        }
        else
        {
            throw UnknownAggregationType("unknown aggregation type: {}", name);
        }
    }
    return aggregationPhysicalFunctions;
}
}

RewriteRuleResultSubgraph LowerToPhysicalWindowedAggregation::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<WindowedAggregationLogicalOperator>(), "Expected a WindowedAggregationLogicalOperator");
    PRECONDITION(logicalOperator.getInputOriginIds().size() == 1, "Expected one origin id vector");
    PRECONDITION(logicalOperator.getOutputOriginIds().size() == 1, "Expected one output origin id");
    PRECONDITION(logicalOperator.getInputSchemas().size() == 1, "Expected one input schema");

    auto aggregation = logicalOperator.get<WindowedAggregationLogicalOperator>();
    auto handlerId = getNextOperatorHandlerId();
    auto outputSchema = aggregation.getOutputSchema();
    auto inputOriginIds = aggregation.getInputOriginIds()[0];
    auto outputOriginId = aggregation.getOutputOriginIds()[0];
    auto timeFunction = getTimeFunction(aggregation);
    auto windowType = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(aggregation.getWindowType());
    INVARIANT(windowType != nullptr, "Window type must be a time-based window type");
    auto aggregationPhysicalFunctions = getAggregationPhysicalFunctions(aggregation, conf);

    const auto valueSize = std::accumulate(
        aggregationPhysicalFunctions.begin(),
        aggregationPhysicalFunctions.end(),
        0,
        [](const auto& sum, const auto& function) { return sum + function->getSizeOfStateInBytes(); });

    uint64_t keySize = 0;
    std::vector<PhysicalFunction> keyFunctions;
    auto newInputSchema = aggregation.getInputSchemas()[0];
    for (auto& nodeFunctionKey : aggregation.getGroupingKeys())
    {
        auto loweredFunctionType = nodeFunctionKey.getDataType();
        if (loweredFunctionType.isType(DataType::Type::VARSIZED))
        {
            loweredFunctionType.type = DataType::Type::VARSIZED_POINTER_REP;
            const bool fieldReplaceSuccess = newInputSchema.replaceTypeOfField(nodeFunctionKey.getFieldName(), loweredFunctionType);
            INVARIANT(fieldReplaceSuccess, "Expect to change the type of {} for {}", nodeFunctionKey.getFieldName(), newInputSchema);
        }
        keyFunctions.emplace_back(QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionKey));
        keySize += DataTypeProvider::provideDataType(loweredFunctionType.type).getSizeInBytes();
    }
    const auto entrySize = sizeof(Interface::ChainedHashMapEntry) + keySize + valueSize;
    const auto numberOfBuckets = conf.numberOfPartitions.getValue();
    const auto pageSize = conf.pageSize.getValue();
    const auto entriesPerPage = pageSize / entrySize;

    const auto& [fieldKeyNames, fieldValueNames] = getKeyAndValueFields(aggregation);
    
    // For serializable aggregation, we need different field offset handling
    bool serializableEnabled = conf.enableSerializableAggregation.getValue();
    std::vector<Interface::MemoryProvider::FieldOffsets> fieldKeys, fieldValues;
    
    if (serializableEnabled) {
        // For OffsetHashMap with aggregation: keys use input schema, values should be empty
        // This allows OffsetEntryMemoryProvider to calculate correct aggregation state offsets
        auto [keys, values] = Interface::MemoryProvider::OffsetEntryMemoryProvider::createOffsetFieldOffsets(
            newInputSchema, fieldKeyNames, {} // Empty fieldValueNames for aggregation
        );
        
        // Convert OffsetFieldOffsets to FieldOffsets for compatibility
        for (const auto& key : keys) {
            fieldKeys.emplace_back(Interface::MemoryProvider::FieldOffsets{
                key.fieldIdentifier, key.type, key.fieldOffset
            });
        }
        // Process values too - should be empty for aggregation states
        for (const auto& val : values) {
            fieldValues.emplace_back(Interface::MemoryProvider::FieldOffsets{
                val.fieldIdentifier, val.type, val.fieldOffset
            });
        }
        printf("FIELD_DEBUG: Serializable aggregation - fieldValues size=%zu after processing\n", fieldValues.size());
        if (!fieldValues.empty()) {
            printf("FIELD_DEBUG: WARNING - fieldValues should be empty for aggregation but has %zu entries\n", fieldValues.size());
        }
        fflush(stdout);
    } else {
        // Regular ChainedHashMap logic
        auto [keys, values] = Interface::MemoryProvider::ChainedEntryMemoryProvider::createFieldOffsets(
            newInputSchema, fieldKeyNames, fieldValueNames
        );
        fieldKeys = std::move(keys);
        fieldValues = std::move(values);
    }

    // Debug: Log aggregation hash map configuration and field offsets
    NES_DEBUG(
        "AGG_FIELD: keySize={} valueSize={} entriesPerPage={} entrySize={} buckets={} pageSize={} serializableEnabled={}",
        keySize,
        valueSize,
        entriesPerPage,
        entrySize,
        numberOfBuckets,
        pageSize,
        serializableEnabled);
    for (const auto& f : fieldKeys) {
        NES_DEBUG(
            "AGG_FIELD_OFFSETS: key field='{}' offset={} size={}",
            f.fieldIdentifier,
            f.fieldOffset,
            f.type.getSizeInBytes());
    }
    for (const auto& f : fieldValues) {
        NES_DEBUG(
            "AGG_FIELD_OFFSETS: value field='{}' offset={} size={}",
            f.fieldIdentifier,
            f.fieldOffset,
            f.type.getSizeInBytes());
    }

    const auto windowMetaData = WindowMetaData{aggregation.getWindowStartFieldName(), aggregation.getWindowEndFieldName()};

    const HashMapOptions hashMapOptions(
        std::make_unique<Interface::MurMur3HashFunction>(),
        keyFunctions,
        fieldKeys,
        fieldValues,
        entriesPerPage,
        entrySize,
        keySize,
        valueSize,
        pageSize,
        numberOfBuckets);

    auto sliceAndWindowStore
        = std::make_unique<DefaultTimeBasedSliceStore>(windowType->getSize().getTime(), windowType->getSlide().getTime());
    
    // Check configuration to decide which handler to create
    std::shared_ptr<OperatorHandler> handler;
    
    if (serializableEnabled) {
        // Create SerializableAggregationOperatorHandler and configure sizes
        auto serializableHandler = std::make_shared<SerializableAggregationOperatorHandler>(
            inputOriginIds, outputOriginId, std::move(sliceAndWindowStore)
        );
        // Propagate correct key/value sizes and parameters into handler state config
        auto& serState = serializableHandler->getState();
        serState.config.keySize = keySize;               // sum of key sizes
        serState.config.valueSize = valueSize;           // sum of aggregation state sizes
        serState.config.numberOfBuckets = numberOfBuckets;
        serState.config.pageSize = pageSize;
        handler = serializableHandler;
        NES_DEBUG("AGG_HANDLER: Using SerializableAggregationOperatorHandler (keySize={}, valueSize={}, buckets={}, pageSize={})",
                  keySize, valueSize, numberOfBuckets, pageSize);
    } else {
        // Create regular AggregationOperatorHandler
        handler = std::make_shared<AggregationOperatorHandler>(
            inputOriginIds, outputOriginId, std::move(sliceAndWindowStore)
        );
        NES_DEBUG("AGG_HANDLER: Using AggregationOperatorHandler");
    }
    auto build = AggregationBuildPhysicalOperator(
        handlerId,
        std::move(timeFunction),
        aggregationPhysicalFunctions,
        hashMapOptions,
        serializableEnabled);
    auto probe = AggregationProbePhysicalOperator(
        hashMapOptions,
        aggregationPhysicalFunctions,
        handlerId,
        windowMetaData,
        serializableEnabled);

    auto buildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        build, newInputSchema, outputSchema, handlerId, handler, PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        probe,
        newInputSchema,
        outputSchema,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{buildWrapper});

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), buildWrapper);
    return {.root = probeWrapper, .leafs = {leafes}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterWindowedAggregationRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalWindowedAggregation>(argument.conf);
}
}
