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
#include <RewriteRules/LowerToPhysical/LowerToPhysicalHashJoin.hpp>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <ranges>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Join/HashJoin/HJBuildPhysicalOperator.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/HashJoin/HJProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/TimeFunction.hpp>
#include <Watermark/TimestampField.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <ErrorHandling.hpp>
#include <HashMapOptions.hpp>
#include <MapPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES
{

namespace
{
/// Helper struct for storing the old and new field name and datatype for each join comparison
struct FieldNamesExtension
{
    std::string oldName;
    std::string newName;
    DataType oldDataType;
    DataType newDataType;
};

std::pair<std::vector<FieldNamesExtension>, std::vector<FieldNamesExtension>>
getJoinFieldExtensionsLeftRight(const Schema& leftInputSchema, const Schema& rightInputSchema, LogicalFunction& joinFunction)
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
        [leftInputSchema, rightInputSchema, &leftJoinNames, &rightJoinNames, &counter](const LogicalFunction& parent)
        {
            /// We expect the parent to have exactly two children and that both children are FieldAccessLogicalFunction
            /// This should be true, as the join operator receives an input schema from its parent operator without any additional functions
            /// over the join fields.
            PRECONDITION(parent.getChildren().size() == 2, "Expect the parent to have exact two children, left and right join fields");
            const auto& firstChild = parent.getChildren().at(0).tryGet<FieldAccessLogicalFunction>();
            const auto& secondChild = parent.getChildren().at(1).tryGet<FieldAccessLogicalFunction>();
            if (not(firstChild.has_value() && secondChild.has_value()))
            {
                throw UnknownJoinStrategy(
                    "Could not handle join strategy that has chained logical functions operating over the join fields!");
            }
            const auto leftField = leftInputSchema.getFieldByName(firstChild->getFieldName()).has_value() ? *firstChild : *secondChild;
            const auto rightField = rightInputSchema.getFieldByName(firstChild->getFieldName()).has_value() ? *firstChild : *secondChild;

            /// If they do not have the same data types, we need to cast both to a common one
            if (firstChild->getDataType() != secondChild->getDataType())
            {
                /// We are now converting the fields to a physical data type and then joining them together
                const auto joinedDataType = leftField.getDataType().join(rightField.getDataType());
                if (joinedDataType.has_value())
                {
                    const auto leftFieldNewName = leftField.getFieldName() + "_" + std::to_string(counter++);
                    const auto rightFieldNewName = rightField.getFieldName() + "_" + std::to_string(counter++);
                    leftJoinNames.emplace_back(FieldNamesExtension{
                        .oldName = leftField.getFieldName(),
                        .newName = leftFieldNewName,
                        .oldDataType = leftField.getDataType(),
                        .newDataType = *joinedDataType});
                    rightJoinNames.emplace_back(FieldNamesExtension{
                        .oldName = rightField.getFieldName(),
                        .newName = rightFieldNewName,
                        .oldDataType = rightField.getDataType(),
                        .newDataType = *joinedDataType});
                }
            }
            else
            {
                leftJoinNames.emplace_back(FieldNamesExtension{
                    .oldName = leftField.getFieldName(),
                    .newName = leftField.getFieldName(),
                    .oldDataType = leftField.getDataType(),
                    .newDataType = leftField.getDataType()});
                rightJoinNames.emplace_back(FieldNamesExtension{
                    .oldName = rightField.getFieldName(),
                    .newName = rightField.getFieldName(),
                    .oldDataType = rightField.getDataType(),
                    .newDataType = rightField.getDataType()});
            }
        });

    return {leftJoinNames, rightJoinNames};
}

/// Creates for each field a map operator that has as its function a cast to the correct data type
std::pair<Schema, std::vector<std::shared_ptr<PhysicalOperatorWrapper>>>
addMapOperators(const Schema& inputSchemaOfJoin, const std::vector<FieldNamesExtension>& fieldNameExtensions)
{
    Schema inputSchemaOfMap(inputSchemaOfJoin);
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> mapPhysicalOperators;
    for (const auto& [oldName, newName, oldDataType, newDataType] : fieldNameExtensions)
    {
        if (newName == oldName and newDataType == oldDataType)
        {
            continue;
        }

        /// Creating a new physical function that reads from the old field and casts it to the new data type
        const FieldAccessLogicalFunction fieldAccessOldField(oldDataType, oldName);
        const CastToTypeLogicalFunction castToTypeFunction(newDataType, fieldAccessOldField);
        const PhysicalFunction castedPhysicalFunction = QueryCompilation::FunctionProvider::lowerFunction(castToTypeFunction);

        /// Get a copy of the current input schema before adding to the inputSchemaOfMap the newly added field
        const Schema copyOfInputSchemaOfMap(inputSchemaOfMap);
        inputSchemaOfMap.addField(newName, newDataType);

        /// Create a new map operator with the cast as its function
        mapPhysicalOperators.emplace_back(std::make_shared<PhysicalOperatorWrapper>(
            MapPhysicalOperator(newName, castedPhysicalFunction), copyOfInputSchemaOfMap, inputSchemaOfMap));
    }

    return {inputSchemaOfMap, mapPhysicalOperators};
}

HashMapOptions
createHashMapOptions(std::vector<FieldNamesExtension>& joinFieldExtensions, Schema& inputSchema, const QueryExecutionConfiguration& conf)
{
    uint64_t keySize = 0;
    constexpr auto valueSize = sizeof(PagedVector);
    std::vector<PhysicalFunction> keyFunctions;
    std::vector<std::string> fieldKeyNames;
    for (auto& fieldExtension : joinFieldExtensions)
    {
        const FieldAccessLogicalFunction fieldAccessKey{fieldExtension.newDataType, fieldExtension.newName};
        if (fieldExtension.newDataType.isType(DataType::Type::VARSIZED))
        {
            fieldExtension.newDataType.type = DataType::Type::VARSIZED_POINTER_REP;
            const bool fieldReplaceSuccess = inputSchema.replaceTypeOfField(fieldExtension.newName, fieldExtension.newDataType);
            INVARIANT(fieldReplaceSuccess, "Expect to change the type of {} for {}", fieldExtension.newName, inputSchema);
        }
        keySize += fieldExtension.newDataType.getSizeInBytes();
        keyFunctions.emplace_back(QueryCompilation::FunctionProvider::lowerFunction(fieldAccessKey));
        fieldKeyNames.emplace_back(fieldExtension.newName);
    }

    const auto pageSize = conf.pageSize.getValue();
    const auto numberOfBuckets = conf.numberOfPartitions.getValue();
    const auto entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;
    const auto entriesPerPage = pageSize / entrySize;

    /// As we are using a paged vector for the value, we do not need to set the fieldNameValues for the chained hashmap
    const auto& [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(inputSchema, fieldKeyNames, {});
    HashMapOptions hashMapOptions{
        std::make_unique<MurMur3HashFunction>(),
        std::move(keyFunctions),
        fieldKeys,
        fieldValues,
        entriesPerPage,
        entrySize,
        keySize,
        valueSize,
        pageSize,
        numberOfBuckets};
    return hashMapOptions;
}
}

RewriteRuleResultSubgraph LowerToPhysicalHashJoin::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<JoinLogicalOperator>(), "Expected a JoinLogicalOperator");
    PRECONDITION(std::ranges::size(logicalOperator.getChildren()) == 2, "Expected two children");
    auto outputOriginIdsOpt = getTrait<OutputOriginIdsTrait>(logicalOperator.getTraitSet());
    PRECONDITION(outputOriginIdsOpt.has_value(), "Expected the outputOriginIds trait to be set");
    auto& outputOriginIds = outputOriginIdsOpt.value();
    PRECONDITION(std::ranges::size(outputOriginIdsOpt.value()) == 1, "Expected one output origin id");
    PRECONDITION(logicalOperator.getInputSchemas().size() == 2, "Expected two input schemas");

    auto join = logicalOperator.getAs<JoinLogicalOperator>();

    auto outputSchema = join.getOutputSchema();
    auto outputOriginId = outputOriginIds[0];
    auto logicalJoinFunction = join->getJoinFunction();
    auto windowType = NES::as<Windowing::TimeBasedWindowType>(join->getWindowType());
    auto [timeStampFieldLeft, timeStampFieldRight] = TimestampField::getTimestampLeftAndRight(join.get(), windowType);
    auto physicalJoinFunction = QueryCompilation::FunctionProvider::lowerFunction(logicalJoinFunction);
    const auto inputOriginIds
        = join.getChildren()
        | std::views::transform(
              [](const auto& child)
              {
                  auto childOutputOriginIds = getTrait<OutputOriginIdsTrait>(child.getTraitSet());
                  PRECONDITION(childOutputOriginIds.has_value(), "Expected the outputOriginIds trait of the child to be set");
                  return childOutputOriginIds.value();
              })
        | std::views::join | std::ranges::to<std::vector<OriginId>>();

    /// Our current hash join implementation uses a hash table that requires each key to be 100% identical in terms of no. fields and data types.
    /// Therefore, we need to create map operators that extend and cast the fields to the correct data types.
    /// TODO #976 we need to have the wrong order of the join input schemas. Inputschema[0] is the left and inputSchema[1] is the right one
    auto [leftJoinFields, rightJoinFields]
        = getJoinFieldExtensionsLeftRight(join->getLeftSchema(), join->getRightSchema(), logicalJoinFunction);
    auto [newLeftInputSchema, leftMapOperators] = addMapOperators(join->getLeftSchema(), leftJoinFields);
    auto [newRightInputSchema, rightMapOperators] = addMapOperators(join->getRightSchema(), rightJoinFields);
    auto leftBufferRef
        = TupleBufferRef::create(conf.numberOfRecordsPerKey.getValue() * newLeftInputSchema.getSizeOfSchemaInBytes(), newLeftInputSchema);
    auto rightBufferRef
        = TupleBufferRef::create(conf.numberOfRecordsPerKey.getValue() * newRightInputSchema.getSizeOfSchemaInBytes(), newRightInputSchema);
    auto leftHashMapOptions = createHashMapOptions(leftJoinFields, newLeftInputSchema, conf);
    auto rightHashMapOptions = createHashMapOptions(rightJoinFields, newRightInputSchema, conf);

    /// Creating the left and right hash join build operator
    auto handlerId = getNextOperatorHandlerId();
    const HJBuildPhysicalOperator leftBuildOperator{
        handlerId, JoinBuildSideType::Left, timeStampFieldLeft.toTimeFunction(), leftBufferRef, leftHashMapOptions};
    const HJBuildPhysicalOperator rightBuildOperator{
        handlerId, JoinBuildSideType::Right, timeStampFieldRight.toTimeFunction(), rightBufferRef, rightHashMapOptions};

    /// Creating the hash join probe
    auto joinSchema = JoinSchema(newLeftInputSchema, newRightInputSchema, outputSchema);
    auto probeOperator = HJProbePhysicalOperator(
        handlerId,
        physicalJoinFunction,
        join->getWindowMetaData(),
        joinSchema,
        leftBufferRef,
        rightBufferRef,
        leftHashMapOptions,
        rightHashMapOptions);


    /// Creating the hash join operator handler
    auto sliceAndWindowStore
        = std::make_unique<DefaultTimeBasedSliceStore>(windowType->getSize().getTime(), windowType->getSlide().getTime());
    auto handler
        = std::make_shared<HJOperatorHandler>(inputOriginIds, outputOriginId, std::move(sliceAndWindowStore), conf.maxNumberOfBuckets);


    /// Building operator wrapper for the two builds and the probe.
    auto leftBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(leftBuildOperator),
        newLeftInputSchema,
        outputSchema,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto rightBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(rightBuildOperator),
        newRightInputSchema,
        outputSchema,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(probeOperator),
        outputSchema,
        outputSchema,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{leftBuildWrapper, rightBuildWrapper});

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

    return {.root = {probeWrapper}, .leafs = {leftLeaf, rightLeaf}};
};

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterHashJoinRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalHashJoin>(argument.conf);
}

}
