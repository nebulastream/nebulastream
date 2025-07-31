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
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
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
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <ErrorHandling.hpp>
#include <HashMapOptions.hpp>
#include <MapPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include "DataTypes/UnboundSchema.hpp"
#include "Util/SchemaFactory.hpp"

namespace NES
{

namespace
{
/// Helper struct for storing the old and new field name and datatype for each join comparison
struct FieldNamesExtension
{
    Field oldField;
    UnboundField newField;
};

std::pair<std::vector<FieldNamesExtension>, std::vector<FieldNamesExtension>>
getJoinFieldExtensionsLeftRight(const LogicalOperator& leftChild, const LogicalOperator& rightChild, LogicalFunction& joinFunction)
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
            const auto& firstChild = parent.getChildren().at(0).tryGet<FieldAccessLogicalFunction>();
            const auto& secondChild = parent.getChildren().at(1).tryGet<FieldAccessLogicalFunction>();
            if (not(firstChild.has_value() && secondChild.has_value()))
            {
                throw UnknownJoinStrategy(
                    "Could not handle join strategy that has chained logical functions operating over the join fields!");
            }
            const auto leftField = firstChild.value().getField().getProducedBy() == leftChild ? firstChild.value().getField()
                                                                                              : secondChild.value().getField();
            const auto rightField = firstChild.value().getField().getProducedBy() == rightChild ? firstChild.value().getField()
                                                                                                : secondChild.value().getField();
            if (leftField.getProducedBy() == rightField.getProducedBy())
            {
                throw UnknownJoinStrategy("Cannot handle self joins yet, but got {} as part of the predicate", joinFunction);
            }

            /// If they do not have the same data types, we need to cast both to a common one
            if (firstChild->getDataType() != secondChild->getDataType())
            {
                /// We are now converting the fields to a physical data type and then joining them together
                if (auto joinedDataType = leftField.getDataType().join(rightField.getDataType()); joinedDataType.has_value())
                {
                    if (joinedDataType->isType(DataType::Type::VARSIZED))
                    {
                        joinedDataType = DataType{DataType::Type::VARSIZED_POINTER_REP};
                    }
                    const auto leftFieldNewName
                        = IdentifierList::create(leftField.getLastName(), Identifier::parse("j" + std::to_string(counter++)));
                    const auto rightFieldNewName
                        = IdentifierList::create(rightField.getLastName(), Identifier::parse("j" + std::to_string(counter++)));
                    leftJoinNames.emplace_back(
                        FieldNamesExtension{.oldField = leftField, .newField = UnboundField{leftFieldNewName, *joinedDataType}});
                    rightJoinNames.emplace_back(
                        FieldNamesExtension{.oldField = rightField, .newField = UnboundField{rightFieldNewName, *joinedDataType}});
                }
            }
            else
            {
                leftJoinNames.emplace_back(
                    FieldNamesExtension{.oldField = leftField, .newField = UnboundField{leftField.getLastName(), leftField.getDataType()}});
                rightJoinNames.emplace_back(
                    FieldNamesExtension{
                        .oldField = rightField, .newField = UnboundField{rightField.getLastName(), rightField.getDataType()}});
            }
        });

    return {leftJoinNames, rightJoinNames};
}

/// Creates for each field a map operator that has as its function a cast to the correct data type
std::pair<UnboundOrderedSchema, std::vector<std::shared_ptr<PhysicalOperatorWrapper>>> addMapOperators(
    const LogicalOperator& inputOperator,
    const std::vector<FieldNamesExtension>& fieldNameExtensions,
    const MemoryLayoutType& memoryLayoutType)
{
    auto currentFields = unbind(inputOperator.getOutputSchema()) | std::ranges::to<std::vector<UnboundField>>();
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
        const PhysicalFunction castedPhysicalFunction = QueryCompilation::FunctionProvider::lowerFunction(castToTypeFunction);

        /// Get a copy of the current input schema before adding to the inputSchemaOfMap the newly added field
        auto inputSchema = UnboundOrderedSchema{currentFields};
        currentFields.emplace_back(newField);
        const UnboundOrderedSchema outputSchema(currentFields);

        /// Create a new map operator with the cast as its function
        mapPhysicalOperators.emplace_back(
            std::make_shared<PhysicalOperatorWrapper>(
                MapPhysicalOperator(newField.getFullyQualifiedName(), castedPhysicalFunction),
                inputSchema,
                outputSchema,
                memoryLayoutType,
                memoryLayoutType));
        // mapPhysicalOperators.emplace_back(std::make_shared<PhysicalOperatorWrapper>(
        //     MapPhysicalOperator(newName, castedPhysicalFunction),
        //     copyOfInputSchemaOfMap,
        //     inputSchemaOfMap,
        //     memoryLayoutType,
        //     memoryLayoutType));
    }

    return {UnboundOrderedSchema{currentFields}, mapPhysicalOperators};
}

HashMapOptions createHashMapOptions(
    std::vector<FieldNamesExtension>& joinFieldExtensions, UnboundOrderedSchema& inputSchema, const QueryExecutionConfiguration& conf)
{
    uint64_t keySize = 0;
    constexpr auto valueSize = sizeof(PagedVector);
    std::vector<PhysicalFunction> keyFunctions;
    std::vector<IdentifierList> fieldKeyNames;
    for (auto& fieldExtension : joinFieldExtensions)
    {
        keySize += fieldExtension.newField.getDataType().getSizeInBytes();
        keyFunctions.emplace_back(FieldAccessPhysicalFunction{fieldExtension.newField.getFullyQualifiedName()});
        fieldKeyNames.emplace_back(fieldExtension.newField.getFullyQualifiedName());
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
    auto join = logicalOperator.getAs<JoinLogicalOperator>();
    const auto children = join->getBothChildren();
    const auto traitSet = join->getTraitSet();
    auto outputOriginIds = traitSet.get<OutputOriginIdsTrait>();
    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait.memoryLayout;
    PRECONDITION(std::ranges::size(outputOriginIds) == 1, "Expected one output origin id");

    const auto& leftOperator = children[0];
    const auto& rightOperator = children[1];

    const auto logicalOutputSchema = join.getOutputSchema();
    const auto physicalOutputSchema = createPhysicalOutputSchema(traitSet);
    auto outputOriginId = outputOriginIds[0];
    auto logicalJoinFunction = join->getJoinFunction();
    auto windowType = NES::as<Windowing::TimeBasedWindowType>(join->getWindowType());
    const auto& joinTimeCharacteristicsVariant = join->getJoinTimeCharacteristics();
    auto characteristicsAreBound
        = std::holds_alternative<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristicsVariant);
    PRECONDITION(characteristicsAreBound, "Expected the join time characteristics to be bound");
    auto& [timeStampFieldLeft, timeStampFieldRight]
        = std::get<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristicsVariant);

    auto physicalJoinFunction = QueryCompilation::FunctionProvider::lowerFunction(logicalJoinFunction);
    const auto inputOriginIds
        = join.getChildren()
        | std::views::transform(
              [](const auto& child)
              {
                  auto childOutputOriginIds = child.getTraitSet().template get<OutputOriginIdsTrait>();
                  return childOutputOriginIds;
              })
        | std::views::join | std::ranges::to<std::vector<OriginId>>();

    /// Our current hash join implementation uses a hash table that requires each key to be 100% identical in terms of no. fields and data types.
    /// Therefore, we need to create map operators that extend and cast the fields to the correct data types.
    /// TODO #976 we need to have the wrong order of the join input schemas. Inputschema[0] is the left and inputSchema[1] is the right one
    auto [leftJoinFields, rightJoinFields] = getJoinFieldExtensionsLeftRight(leftOperator, rightOperator, logicalJoinFunction);
    auto [newLeftInputSchema, leftMapOperators] = addMapOperators(leftOperator, leftJoinFields, memoryLayoutType);
    auto [newRightInputSchema, rightMapOperators] = addMapOperators(rightOperator, rightJoinFields, memoryLayoutType);
    auto sizeOfSchema = [](const UnboundOrderedSchema& schema)
    {
        return std::ranges::fold_left(
            schema | std::views::transform([](const auto& field) { return field.getDataType().getSizeInBytes(); }), 0, std::plus{});
    };
    /// TODO decide memory layout in optimization stage and pass it here
    auto leftBufferRef = LowerSchemaProvider::lowerSchema(
        conf.numberOfRecordsPerKey.getValue() * newLeftInputSchema.getSizeInBytes(), newLeftInputSchema, memoryLayoutType);
    auto rightBufferRef = LowerSchemaProvider::lowerSchema(
        conf.numberOfRecordsPerKey.getValue() * newRightInputSchema.getSizeInBytes(), newRightInputSchema, memoryLayoutType);
    auto leftHashMapOptions = createHashMapOptions(leftJoinFields, newLeftInputSchema, conf);
    auto rightHashMapOptions = createHashMapOptions(rightJoinFields, newRightInputSchema, conf);

    /// Creating the left and right hash join build operator
    auto handlerId = getNextOperatorHandlerId();
    const HJBuildPhysicalOperator leftBuildOperator{
        handlerId, JoinBuildSideType::Left, TimeFunction::create(timeStampFieldLeft), leftBufferRef, leftHashMapOptions};
    const HJBuildPhysicalOperator rightBuildOperator{
        handlerId, JoinBuildSideType::Right, TimeFunction::create(timeStampFieldRight), rightBufferRef, rightHashMapOptions};

    /// Creating the hash join probe
    auto joinSchema = JoinSchema(newLeftInputSchema, newRightInputSchema, physicalOutputSchema);
    auto probeOperator = HJProbePhysicalOperator(
        handlerId,
        physicalJoinFunction,
        WindowMetaData{join->getStartField(), join->getEndField()},
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

    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(probeOperator),
        physicalOutputSchema,
        physicalOutputSchema,
        memoryLayoutType,
        memoryLayoutType,
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
