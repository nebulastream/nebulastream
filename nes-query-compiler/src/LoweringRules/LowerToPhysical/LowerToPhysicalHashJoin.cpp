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
#include <LoweringRules/LowerToPhysical/LowerToPhysicalHashJoin.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/Hash/BloomFilterRef.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Join/HashJoin/HJBuildPhysicalOperator.hpp>
#include <Join/HashJoin/HJInnerProbePhysicalOperator.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/HashJoin/HJOuterProbePhysicalOperator.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/JoinTriggerStrategy.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>
#include <LoweringRuleRegistry.hpp>
#include <MapPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

namespace
{
/// Helper struct for storing the old and new field name and datatype for each join comparison
struct FieldNamesExtension
{
    Field oldField;
    QualifiedUnboundField newField;
};

/// Sizing of the join's in-map BloomFilter, or nullopt when it is switched off.
///
/// Sized from expectedEntries rather than from the numberOfBuckets these options carry, because the bucket
/// count bounds nothing: the hash maps never rehash, they only lengthen their chains, so a map routinely
/// holds far more keys than it has buckets. Sizing the filter for the bucket count would saturate every bit
/// and make mightContain() always true, i.e. pay the hash positions and skip nothing.
std::optional<Nautilus::Interface::BloomFilterParams> createBloomFilterParams(const QueryExecutionConfiguration& conf)
{
    if (not conf.bloomFilterConfiguration.enableBloomFilter)
    {
        return std::nullopt;
    }
    return Nautilus::Interface::BloomFilterParams{
        conf.bloomFilterConfiguration.expectedEntries, conf.bloomFilterConfiguration.falsePositiveRate};
}

std::pair<std::vector<FieldNamesExtension>, std::vector<FieldNamesExtension>>
getJoinFieldExtensionsLeftRight(const LogicalOperator& leftChild, const LogicalOperator& rightChild, const LogicalFunction& joinFunction)
{
    /// Tuple  of left, right join fields and the combined data type, e.g., i32 and i8 --> i32
    std::vector<FieldNamesExtension> leftJoinNames;
    std::vector<FieldNamesExtension> rightJoinNames;

    /// Retrieves all leaf functions, as we need the leaf functions (join comparison) to check if they have the same number and data types
    /// for both join sides.
    std::unordered_set<LogicalFunction> parentsOfJoinComparisons;
    for (auto itr : BFSRange<LogicalFunction>(joinFunction))
    {
        /// If any child is a leaf function, we put the current function into the set
        const auto anyChildIsLeaf
            = std::ranges::any_of(itr.getChildren(), [](const LogicalFunction& child) { return child.getChildren().empty(); });
        if (anyChildIsLeaf)
        {
            parentsOfJoinComparisons.insert(itr);
        }
    }
    uint64_t counter = 0;
    std::ranges::for_each(
        parentsOfJoinComparisons,
        [leftChild, rightChild, &leftJoinNames, &rightJoinNames, &counter, &joinFunction](const LogicalFunction& parent)
        {
            /// We expect the parent to have exactly two children and that both children are FieldAccessLogicalFunction
            /// This should be true, as the join operator receives an input schema from its parent operator without any additional functions
            /// over the join fields.
            PRECONDITION(parent.getChildren().size() == 2, "Expect the parent to have exact two children, left and right join fields");
            const auto& firstField = parent.getChildren().at(0).tryGetAs<FieldAccessLogicalFunction>();
            const auto& secondField = parent.getChildren().at(1).tryGetAs<FieldAccessLogicalFunction>();
            if (not(firstField.has_value() && secondField.has_value()))
            {
                throw UnknownJoinStrategy(
                    "Could not handle join strategy that has chained logical functions operating over the join fields!");
            }

            auto [leftField, rightField] = [&]
            {
                if (firstField.value()->getField().getProducedBy() == leftChild)
                {
                    PRECONDITION(
                        secondField.value()->getField().getProducedBy() == rightChild, "Expected the second field to be the right field");
                    return std::pair{firstField.value()->getField(), secondField.value()->getField()};
                }
                PRECONDITION(
                    firstField.value()->getField().getProducedBy() == rightChild, "Expected the first field to be the right field");
                PRECONDITION(
                    secondField.value()->getField().getProducedBy() == leftChild, "Expected the second field to be the left field");
                return std::pair{secondField.value()->getField(), firstField.value()->getField()};
            }();
            if (leftField.getProducedBy() == rightField.getProducedBy())
            {
                throw UnknownJoinStrategy("Cannot handle self joins yet, but got {} as part of the predicate", joinFunction);
            }

            /// If they do not have the same data types, we need to cast both to a common one
            if (firstField->getDataType() != secondField->getDataType())
            {
                /// We are now converting the fields to a physical data type and then joining them together
                if (auto joinedDataType = leftField.getDataType().join(rightField.getDataType()); joinedDataType.has_value())
                {
                    const auto leftFieldNewName
                        = QualifiedIdentifier::create(leftField.getLastName(), Identifier::parse("j" + std::to_string(counter++)));
                    const auto rightFieldNewName
                        = QualifiedIdentifier::create(rightField.getLastName(), Identifier::parse("j" + std::to_string(counter++)));
                    leftJoinNames.emplace_back(
                        FieldNamesExtension{.oldField = leftField, .newField = QualifiedUnboundField{leftFieldNewName, *joinedDataType}});
                    rightJoinNames.emplace_back(
                        FieldNamesExtension{.oldField = rightField, .newField = QualifiedUnboundField{rightFieldNewName, *joinedDataType}});
                }
                else
                {
                    throw UnknownJoinStrategy("Cannot join field types {} and {}", leftField.getDataType(), rightField.getDataType());
                }
            }
            else
            {
                leftJoinNames.emplace_back(FieldNamesExtension{
                    .oldField = leftField, .newField = QualifiedUnboundField{leftField.getLastName(), leftField.getDataType()}});
                rightJoinNames.emplace_back(FieldNamesExtension{
                    .oldField = rightField, .newField = QualifiedUnboundField{rightField.getLastName(), rightField.getDataType()}});
            }
        });

    return {leftJoinNames, rightJoinNames};
}

/// Creates for each field a map operator that has as its function a cast to the correct data type
std::pair<Schema<QualifiedUnboundField, Ordered>, std::vector<std::shared_ptr<PhysicalOperatorWrapper>>> addMapOperators(
    const LogicalOperator& inputOperator,
    const std::vector<FieldNamesExtension>& fieldNameExtensions,
    const MemoryLayoutType& memoryLayoutType)
{
    auto currentFields = createPhysicalOutputSchema(inputOperator.getTraitSet()) | std::ranges::to<std::vector<QualifiedUnboundField>>();
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> mapPhysicalOperators;
    for (const auto& [oldField, newField] : fieldNameExtensions)
    {
        if (oldField.getLastName() == newField.getFullyQualifiedName() and oldField.getDataType() == newField.getDataType())
        {
            continue;
        }

        /// Creating a new physical function that reads from the old field and casts it to the new data type
        const FieldAccessLogicalFunction fieldAccessOldField(oldField);
        const CastToTypeLogicalFunction castToTypeFunction(newField.getDataType(), fieldAccessOldField);
        const PhysicalFunction castedPhysicalFunction
            = QueryCompilation::FunctionProvider::lowerFunction(castToTypeFunction, *inputOperator.getTraitSet().get<FieldMappingTrait>());

        /// Get a copy of the current input schema before adding to the inputSchemaOfMap the newly added field
        auto inputSchema = Schema<QualifiedUnboundField, Ordered>{currentFields};
        currentFields.emplace_back(newField);
        const Schema<QualifiedUnboundField, Ordered> outputSchema(currentFields);

        /// Create a new map operator with the cast as its function
        mapPhysicalOperators.emplace_back(std::make_shared<PhysicalOperatorWrapper>(
            MapPhysicalOperator(newField.getFullyQualifiedName(), castedPhysicalFunction),
            inputSchema,
            outputSchema,
            memoryLayoutType,
            memoryLayoutType));
    }

    return {Schema<QualifiedUnboundField, Ordered>{currentFields}, mapPhysicalOperators};
}

/// The key functions come back alongside the config rather than inside it: extracting key fields out of an
/// incoming record is build-operator logic, not hash map metadata.
std::pair<ChainedHashMapConfig, std::vector<PhysicalFunction>> createChainedHashMapConfig(
    std::vector<FieldNamesExtension>& joinFieldExtensions,
    Schema<QualifiedUnboundField, Ordered>& inputSchema,
    const QueryExecutionConfiguration& conf)
{
    uint64_t keySize = 0;
    constexpr auto valueSize = sizeof(uint32_t);
    std::vector<PhysicalFunction> keyFunctions;
    std::vector<QualifiedIdentifier> fieldKeyNames;
    for (auto& fieldExtension : joinFieldExtensions)
    {
        keySize += fieldExtension.newField.getDataType().getSizeInBytesWithNull();
        keyFunctions.emplace_back(FieldAccessPhysicalFunction{fieldExtension.newField.getFullyQualifiedName()});
        fieldKeyNames.emplace_back(fieldExtension.newField.getFullyQualifiedName());
    }

    const auto pageSize = conf.pageSize;
    const auto numberOfBuckets = conf.numberOfPartitions;
    const auto entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;

    /// As we are using a paged vector for the value, we do not need to set the fieldNameValues for the chained hashmap
    const auto& [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(inputSchema, fieldKeyNames, {});
    return {
        ChainedHashMapConfig{
            .entrySize = entrySize,
            .numberOfBuckets = numberOfBuckets,
            .pageSize = pageSize,
            .bloomFilterParams = createBloomFilterParams(conf),
            .fieldKeys = fieldKeys,
            .fieldValues = fieldValues,
            .hashFunction = std::make_shared<MurMur3HashFunction>()},
        std::move(keyFunctions)};
}
}

LoweringRuleResultSubgraph LowerToPhysicalHashJoin::apply(LogicalOperator logicalOperator)
{
    auto join = logicalOperator.getAs<JoinLogicalOperator>();
    const auto children = join->getBothChildren();
    const auto traitSet = join->getTraitSet();
    auto outputOriginIds = traitSet.get<OutputOriginIdsTrait>();
    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait->memoryLayout;
    PRECONDITION(std::ranges::size(*outputOriginIds) == 1, "Expected one output origin id");

    const auto& leftOperator = children[0];
    const auto& rightOperator = children[1];

    const auto logicalOutputSchema = join.getOutputSchema();
    const auto physicalOutputSchema = createPhysicalOutputSchema(traitSet);
    auto outputOriginId = (*outputOriginIds)[0];
    auto logicalJoinFunction = join->getJoinFunction();
    auto windowType = join->getWindowType();
    const auto& joinTimeCharacteristicsVariant = join->getJoinTimeCharacteristics();
    auto characteristicsAreBound
        = std::holds_alternative<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristicsVariant);
    PRECONDITION(characteristicsAreBound, "Expected the join time characteristics to be bound");
    const auto& [timeStampFieldLeft, timeStampFieldRight]
        = std::get<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristicsVariant);

    auto combinedFieldMappingVec = join->getChildren()
        | std::views::transform([](const auto& child)
                                { return child.getTraitSet().template get<FieldMappingTrait>()->getUnderlying() | std::views::all; })
        | std::views::join | std::views::common | std::ranges::to<std::unordered_map>();
    auto combinedFieldMapping = FieldMappingTrait{std::move(combinedFieldMappingVec)};

    auto physicalJoinFunction = QueryCompilation::FunctionProvider::lowerFunction(logicalJoinFunction, combinedFieldMapping);
    const auto inputOriginIds = join.getChildren()
        | std::views::transform(
                                    [](const auto& child)
                                    {
                                        auto childOutputOriginIds = child.getTraitSet().template get<OutputOriginIdsTrait>();
                                        return *childOutputOriginIds;
                                    })
        | std::views::join | std::ranges::to<std::vector<OriginId>>();

    /// Our current hash join implementation uses a hash table that requires each key to be 100% identical in terms of no. fields and data types.
    /// Therefore, we need to create map operators that extend and cast the fields to the correct data types.
    auto [leftJoinFields, rightJoinFields] = getJoinFieldExtensionsLeftRight(leftOperator, rightOperator, logicalJoinFunction);
    auto [newLeftInputSchema, leftMapOperators] = addMapOperators(leftOperator, leftJoinFields, memoryLayoutType);
    auto [newRightInputSchema, rightMapOperators] = addMapOperators(rightOperator, rightJoinFields, memoryLayoutType);
    auto leftTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(newLeftInputSchema);
    auto rightTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(newRightInputSchema);
    auto [leftHashMapConfig, leftKeyFunctions] = createChainedHashMapConfig(leftJoinFields, newLeftInputSchema, conf);
    auto [rightHashMapConfig, rightKeyFunctions] = createChainedHashMapConfig(rightJoinFields, newRightInputSchema, conf);

    /// The build stores every distinct key's tuples in its own PagedVector sized by the same page-size knob, so the knob must also hold a
    /// tuple - the same "does one element fit on a page" contract HashMapOptions enforces for entriesPerPage.
    const auto requireTupleFitsOnPage = [](const uint64_t pageSize, const auto& tupleLayout, const std::string_view side)
    {
        if (const auto minimumPageSize = tupleLayout->getMinimumPageSize(); pageSize < minimumPageSize)
        {
            throw QueryCompilerError(
                "The {} join input needs a page size of at least {} bytes, but the configured page size is {}. Increase the page size.",
                side,
                minimumPageSize,
                pageSize);
        }
    };
    requireTupleFitsOnPage(leftHashMapConfig.pageSize, leftTupleLayout, "left");
    requireTupleFitsOnPage(rightHashMapConfig.pageSize, rightTupleLayout, "right");

    /// Creating the hash join operator handler and slice store
    auto handlerId = getNextOperatorHandlerId();
    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(
        windowType.getSize().getTime(), windowType.getSlide().getTime(), conf.sliceCacheConfiguration);
    auto sliceStoreRefLeft = sliceAndWindowStore->createSliceStoreRef(
        [](Slice& slice, const WorkerThreadId workerThreadId, AbstractBufferProvider& bufferProvider)
        {
            auto& hjSlice = dynamic_cast<HJSlice&>(slice);
            return hjSlice.getOrCreateHashMapBufferRefForSide(workerThreadId, JoinBuildSideType::Left, bufferProvider);
        },
        [hashMapConfig = leftHashMapConfig](WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
        {
            const CreateNewHashMapSliceArgs hashMapSliceArgs{hashMapConfig, &bufferProvider};
            return handler.getCreateNewSlicesFunction(hashMapSliceArgs);
        });
    auto sliceStoreRefRight = sliceAndWindowStore->createSliceStoreRef(
        [](Slice& slice, const WorkerThreadId workerThreadId, AbstractBufferProvider& bufferProvider)
        {
            auto& hjSlice = dynamic_cast<HJSlice&>(slice);
            return hjSlice.getOrCreateHashMapBufferRefForSide(workerThreadId, JoinBuildSideType::Right, bufferProvider);
        },
        [hashMapConfig = rightHashMapConfig](WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
        {
            const CreateNewHashMapSliceArgs hashMapSliceArgs{hashMapConfig, &bufferProvider};
            return handler.getCreateNewSlicesFunction(hashMapSliceArgs);
        });
    /// Create the trigger strategy based on join type — determines what probe tasks are emitted at runtime
    const auto currentJoinType = join->getJoinType();
    using JT = JoinLogicalOperator::JoinType;
    auto createTriggerStrategy = [&]() -> JoinTriggerStrategy
    {
        switch (currentJoinType)
        {
            case JT::OUTER_LEFT_JOIN:
                return OuterJoinTriggerStrategy<true, false>{};
            case JT::OUTER_RIGHT_JOIN:
                return OuterJoinTriggerStrategy<false, true>{};
            case JT::OUTER_FULL_JOIN:
                return OuterJoinTriggerStrategy<true, true>{};
            case JT::CARTESIAN_PRODUCT:
            case JT::INNER_JOIN:
                return InnerJoinTriggerStrategy{};
        }
        std::unreachable();
    };

    auto handler
        = std::make_shared<HJOperatorHandler>(inputOriginIds, outputOriginId, std::move(sliceAndWindowStore), createTriggerStrategy());

    /// Creating the left and right hash join build operator
    const HJBuildPhysicalOperator leftBuildOperator{
        handlerId,
        JoinBuildSideType::Left,
        TimeFunction::create(timeStampFieldLeft),
        leftTupleLayout,
        leftHashMapConfig,
        std::move(leftKeyFunctions),
        std::move(sliceStoreRefLeft)};
    const HJBuildPhysicalOperator rightBuildOperator{
        handlerId,
        JoinBuildSideType::Right,
        TimeFunction::create(timeStampFieldRight),
        rightTupleLayout,
        rightHashMapConfig,
        std::move(rightKeyFunctions),
        std::move(sliceStoreRefRight)};

    /// Creating the hash join probe — select inner or outer probe based on join type
    auto joinSchema = JoinSchema(newLeftInputSchema, newRightInputSchema, physicalOutputSchema);

    /// Building operator wrapper for the two builds and the probe.
    auto leftBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(leftBuildOperator),
        newLeftInputSchema,
        physicalOutputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto rightBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(rightBuildOperator),
        newRightInputSchema,
        physicalOutputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    /// Verify at compile time that both probe operators satisfy the JoinProbeOperator concept
    static_assert(JoinProbeOperator<HJInnerProbePhysicalOperator>);
    static_assert(JoinProbeOperator<HJOuterProbePhysicalOperator>);

    auto createProbeWrapper = [&](const auto& probeOperator)
    {
        return std::make_shared<PhysicalOperatorWrapper>(
            std::move(probeOperator),
            physicalOutputSchema,
            physicalOutputSchema,
            memoryLayoutType,
            memoryLayoutType,
            handlerId,
            handler,
            PhysicalOperatorWrapper::PipelineLocation::SCAN,
            std::vector{leftBuildWrapper, rightBuildWrapper});
    };

    std::shared_ptr<PhysicalOperatorWrapper> probeWrapper;
    if (isOuterJoin(currentJoinType))
    {
        PRECONDITION(
            HJOuterProbePhysicalOperator::supportsJoinType(currentJoinType), "HJOuterProbePhysicalOperator does not support join type");
        probeWrapper = createProbeWrapper(HJOuterProbePhysicalOperator(
            handlerId,
            physicalJoinFunction,
            WindowMetaData{join->getStartField(), join->getEndField()},
            joinSchema,
            leftTupleLayout,
            rightTupleLayout,
            leftHashMapConfig,
            rightHashMapConfig));
    }
    else
    {
        PRECONDITION(
            HJInnerProbePhysicalOperator::supportsJoinType(currentJoinType), "HJInnerProbePhysicalOperator does not support join type");
        probeWrapper = createProbeWrapper(HJInnerProbePhysicalOperator(
            handlerId,
            physicalJoinFunction,
            WindowMetaData{join->getStartField(), join->getEndField()},
            joinSchema,
            leftTupleLayout,
            rightTupleLayout,
            leftHashMapConfig,
            rightHashMapConfig));
    }

    std::shared_ptr<PhysicalOperatorWrapper> leftLeaf = leftBuildWrapper;
    std::shared_ptr<PhysicalOperatorWrapper> rightLeaf = rightBuildWrapper;
    /// As we have the query plan still flipped, we need to iterate in reverse for inserting the map operators into the query plan
    if (not leftMapOperators.empty())
    {
        for (const auto& mapPhysicalOperator : leftMapOperators | std::views::reverse)
        {
            leftLeaf->addChild(mapPhysicalOperator);
            leftLeaf = mapPhysicalOperator;
        }
    }
    if (not rightMapOperators.empty())
    {
        for (const auto& mapPhysicalOperator : rightMapOperators | std::views::reverse)
        {
            rightLeaf->addChild(mapPhysicalOperator);
            rightLeaf = mapPhysicalOperator;
        }
    }

    return {.root = {probeWrapper}, .leaves = {leftLeaf, rightLeaf}};
};

}
