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
#include <ranges>
#include <utility>
#include <vector>

#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationProbePhysicalOperator.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/Interface/BufferRef/ColumnTupleBufferRef.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
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

namespace
{
std::vector<std::shared_ptr<AggregationPhysicalFunction>>
getAggregationPhysicalFunctions(const WindowedAggregationLogicalOperator& logicalOperator, const QueryExecutionConfiguration& configuration)
{
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    const auto& aggregationDescriptors = logicalOperator.getWindowAggregation();
    for (const auto& descriptor : aggregationDescriptors)
    {
        auto physicalInputType = DataTypeProvider::provideDataType(descriptor.function->getInputFunction().getDataType().type);
        auto physicalFinalType = DataTypeProvider::provideDataType(descriptor.function->getInputFunction().getDataType().type);

        auto aggregationInputFunction = QueryCompilation::FunctionProvider::lowerFunction(descriptor.function->getInputFunction());
        const auto resultFieldIdentifier = descriptor.name;
        auto layout = std::make_shared<ColumnLayout>(configuration.pageSize.getValue(), logicalOperator.getChild().getOutputSchema());
        auto bufferRef = std::make_shared<Interface::BufferRef::ColumnTupleBufferRef>(layout);

        auto name = descriptor.function->getName();
        auto aggregationArguments = AggregationPhysicalFunctionRegistryArguments(
            std::move(physicalInputType),
            std::move(physicalFinalType),
            std::move(aggregationInputFunction),
            resultFieldIdentifier,
            bufferRef);
        if (auto aggregationPhysicalFunction
            = AggregationPhysicalFunctionRegistry::instance().create(std::string{name}, std::move(aggregationArguments)))
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
    auto windowedAggregationOpt = logicalOperator.tryGetAs<WindowedAggregationLogicalOperator>();
    PRECONDITION(logicalOperator.tryGetAs<WindowedAggregationLogicalOperator>(), "Expected a WindowedAggregationLogicalOperator");
    auto aggregation = windowedAggregationOpt.value();
    auto outputOriginIdsOpt = getTrait<OutputOriginIdsTrait>(aggregation->getTraitSet());
    auto inputOriginIdsOpt = getTrait<OutputOriginIdsTrait>(aggregation->getChild()->getTraitSet());
    PRECONDITION(outputOriginIdsOpt.has_value(), "Expected the outputOriginIds trait to be set");
    PRECONDITION(inputOriginIdsOpt.has_value(), "Expected the inputOriginIds trait to be set");
    auto& outputOriginIds = outputOriginIdsOpt.value();
    PRECONDITION(std::ranges::size(outputOriginIds) == 1, "Expected one output origin id");
    PRECONDITION(
        std::holds_alternative<Windowing::BoundTimeCharacteristic>(aggregation->getCharacteristic()),
        "Expected time characteristic to be bound");

    auto handlerId = getNextOperatorHandlerId();
    auto outputSchema = aggregation.getOutputSchema();
    auto outputOriginId = outputOriginIds[0];
    auto inputOriginIds = inputOriginIdsOpt.value();
    auto timeFunction = TimeFunction::create(std::get<Windowing::BoundTimeCharacteristic>(aggregation->getCharacteristic()));
    auto windowType = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(aggregation->getWindowType());
    INVARIANT(windowType != nullptr, "Window type must be a time-based window type");
    auto aggregationPhysicalFunctions = getAggregationPhysicalFunctions(*aggregation, conf);

    const auto valueSize = std::accumulate(
        aggregationPhysicalFunctions.begin(),
        aggregationPhysicalFunctions.end(),
        0,
        [](const auto& sum, const auto& function) { return sum + function->getSizeOfStateInBytes(); });

    uint64_t keySize = 0;
    std::vector<PhysicalFunction> keyFunctions;
    std::unordered_map<Identifier, UnboundField> newInputFields = aggregation->getChild().getOutputSchema()
        | std::views::transform([](const auto& field)
                                { return std::make_pair(field.getLastName(), UnboundField{field.getLastName(), field.getDataType()}); })
        | std::ranges::to<std::unordered_map>();
    auto groupingKeys = aggregation->getGroupingKeys();
    const auto keysAreBound = std::holds_alternative<std::vector<FieldAccessLogicalFunction>>(groupingKeys);
    PRECONDITION(keysAreBound, "Expected the grouping keys to be bound");
    auto boundGroupingKeys = std::get<std::vector<FieldAccessLogicalFunction>>(groupingKeys);
    for (auto& nodeFunctionKey : boundGroupingKeys)
    {
        auto loweredFunctionType = nodeFunctionKey.getDataType();
        if (loweredFunctionType.isType(DataType::Type::VARSIZED))
        {
            loweredFunctionType.type = DataType::Type::VARSIZED_POINTER_REP;
            auto fieldNode = newInputFields.extract(nodeFunctionKey.getField().getLastName());
            PRECONDITION(!fieldNode.empty(), "Expect to find the field {} in the input schema", nodeFunctionKey.getField().getLastName());
            fieldNode.mapped() = UnboundField{fieldNode.mapped().getName(), loweredFunctionType};
            newInputFields.insert(std::move(fieldNode));
        }
        keyFunctions.emplace_back(QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionKey));
        keySize += DataTypeProvider::provideDataType(loweredFunctionType.type).getSizeInBytes();
    }
    const auto newInputSchema = UnboundSchema{
        newInputFields | std::views::transform([](const auto& pair) { return pair.second; }) | std::ranges::to<std::vector>()};
    const auto entrySize = sizeof(Interface::ChainedHashMapEntry) + keySize + valueSize;
    const auto numberOfBuckets = conf.numberOfPartitions.getValue();
    const auto pageSize = conf.pageSize.getValue();
    const auto entriesPerPage = pageSize / entrySize;

    const auto fieldKeyNames = boundGroupingKeys | std::views::transform([](const auto& field) { return field.getField().getLastName(); });
    const auto fieldValueNames
        = aggregation->getWindowAggregation() | std::views::transform([](const auto& descriptor) { return descriptor.name; });
    const auto& [fieldKeys, fieldValues] = Interface::BufferRef::ChainedEntryMemoryProvider::createFieldOffsets(
        newInputSchema, fieldKeyNames | std::ranges::to<std::vector>(), fieldValueNames | std::ranges::to<std::vector>());

    const auto windowMetaData = WindowMetaData{aggregation->getWindowStartField(), aggregation->getWindowEndField()};

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
    auto handler = std::make_shared<AggregationOperatorHandler>(
        inputOriginIds | std::ranges::to<std::vector>(), outputOriginId, std::move(sliceAndWindowStore), conf.maxNumberOfBuckets);
    auto build = AggregationBuildPhysicalOperator(handlerId, std::move(timeFunction), aggregationPhysicalFunctions, hashMapOptions);
    auto probe = AggregationProbePhysicalOperator(hashMapOptions, aggregationPhysicalFunctions, handlerId, windowMetaData);

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
