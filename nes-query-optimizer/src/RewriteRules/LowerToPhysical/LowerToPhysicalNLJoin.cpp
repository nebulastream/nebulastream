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

#include <cstdint>
#include <memory>
#include <tuple>
#include <utility>
#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalNLJoin.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJBuildPhysicalOperator.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJProbePhysicalOperator.hpp>
#include <Streaming/Join/StreamJoinUtil.hpp>
#include <Util/Common.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES::Optimizer
{

/// A TimestampField is a wrapper around a FieldName and a Unit of time.
/// This enforces fields carrying time values to be evaluated with respect to a specific timeunit.
class TimestampField
{
    enum TimeFunctionType
    {
        EVENT_TIME,
        INGESTION_TIME,
    };

public:
    friend std::ostream& operator<<(std::ostream& os, const TimestampField& obj);

    /// The multiplier is the value which converts from the underlying time value to milliseconds.
    /// E.g. the multiplier for a timestampfield of seconds is 1000
    [[nodiscard]] Windowing::TimeUnit getUnit() const { return unit; }

    [[nodiscard]] const std::string& getName() const { return fieldName; }

    [[nodiscard]] const TimeFunctionType& getTimeFunctionType() const { return timeFunctionType; }

    /// Builds the TimeFunction
    [[nodiscard]] std::unique_ptr<TimeFunction> toTimeFunction() const
    {
        switch (timeFunctionType)
        {
            case EVENT_TIME:
                return std::make_unique<EventTimeFunction>(::NES::Functions::FieldAccessPhysicalFunction(fieldName), unit);
            case INGESTION_TIME:
                return std::make_unique<IngestionTimeFunction>();
        }
    }
    static TimestampField IngestionTime() { return {"IngestionTime", Windowing::TimeUnit(1), INGESTION_TIME}; }
    static TimestampField EventTime(std::string fieldName, Windowing::TimeUnit tm)
    {
        return {std::move(fieldName), std::move(tm), EVENT_TIME};
    }

private:
    std::string fieldName;
    Windowing::TimeUnit unit;
    TimeFunctionType timeFunctionType;
    TimestampField(std::string fieldName, Windowing::TimeUnit unit, TimeFunctionType timeFunctionType)
        : fieldName(std::move(fieldName)), unit(std::move(unit)), timeFunctionType(timeFunctionType)
    {
    }
};


std::tuple<TimestampField, TimestampField>
getTimestampLeftAndRight(const JoinLogicalOperator& joinOperator, std::shared_ptr<Windowing::TimeBasedWindowType> windowType)
{
    if (windowType->getTimeCharacteristic().getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
    {
        NES_DEBUG("Skip eventime identification as we use ingestion time");
        return {TimestampField::IngestionTime(), TimestampField::IngestionTime()};
    }
    else
    {
        /// FIXME Once #3407 is done, we can change this to get the left and right fieldname
        auto timeStampFieldName = windowType->getTimeCharacteristic().field.getName();
        auto timeStampFieldNameWithoutSourceName = timeStampFieldName.substr(timeStampFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR));

        /// Lambda function for extracting the timestamp from a schema
        auto findTimeStampFieldName = [&](const Schema& schema)
        {
            for (const auto& field : schema)
            {
                if (field.getName().find(timeStampFieldNameWithoutSourceName) != std::string::npos)
                {
                    return field.getName();
                }
            }
            return std::string();
        };

        /// Extracting the left and right timestamp
        auto timeStampFieldNameLeft = findTimeStampFieldName(joinOperator.getInputSchemas()[0]);
        auto timeStampFieldNameRight = findTimeStampFieldName(joinOperator.getInputSchemas()[1]);

        INVARIANT(
            !(timeStampFieldNameLeft.empty() || timeStampFieldNameRight.empty()),
            "Could not find timestampfieldname {} in both streams!",
            timeStampFieldNameWithoutSourceName);

        return {
            TimestampField::EventTime(timeStampFieldNameLeft, windowType->getTimeCharacteristic().getTimeUnit()),
            TimestampField::EventTime(timeStampFieldNameRight, windowType->getTimeCharacteristic().getTimeUnit())};
    }
}

void flattenAllChildrenHelper(
    const LogicalFunction& node, std::vector<LogicalFunction>& allChildren, const LogicalFunction& excludedNode, bool allowDuplicate)
{
    for (const auto& currentNode : node.getChildren())
    {
        if (allowDuplicate)
        {
            allChildren.push_back(currentNode);
            flattenAllChildrenHelper(currentNode, allChildren, excludedNode, allowDuplicate);
        }
        else if (currentNode != excludedNode && std::find(allChildren.begin(), allChildren.end(), currentNode) == allChildren.end())
        {
            allChildren.push_back(currentNode);
            flattenAllChildrenHelper(currentNode, allChildren, excludedNode, allowDuplicate);
        }
    }
}

std::vector<LogicalFunction> flattenAllChildren(const LogicalFunction& current, bool withDuplicateChildren)
{
    std::vector<LogicalFunction> allChildren;
    flattenAllChildrenHelper(current, allChildren, current, withDuplicateChildren);
    return allChildren;
}

auto getJoinFieldNames(const Schema inputSchema, const LogicalFunction joinFunction)
{
    std::vector<std::string> joinFieldNames;
    std::vector<std::string> fieldNamesInJoinFunction;
    std::ranges::for_each(
        flattenAllChildren(joinFunction, false),
        [&fieldNamesInJoinFunction](const LogicalFunction& child)
        {
            if (child.tryGet<FieldAccessLogicalFunction>())
            {
                fieldNamesInJoinFunction.push_back(child.get<FieldAccessLogicalFunction>().getFieldName());
            }
        });

    for (const auto& field : inputSchema)
    {
        if (std::ranges::find(fieldNamesInJoinFunction, field.getName()) != fieldNamesInJoinFunction.end())
        {
            joinFieldNames.push_back(field.getName());
        }
    }
    return joinFieldNames;
};


RewriteRuleResultSubgraph LowerToPhysicalNLJoin::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<JoinLogicalOperator>(), "Expected a JoinLogicalOperator");
    PRECONDITION(logicalOperator.getInputOriginIds().size() == 2, "Expected two origin id vector");
    PRECONDITION(logicalOperator.getOutputOriginIds().size() == 1, "Expected one output origin id");
    PRECONDITION(logicalOperator.getInputSchemas().size() == 2, "Expected two input schemas");

    auto join = logicalOperator.get<JoinLogicalOperator>();
    auto handlerId = getNextOperatorHandlerId();
    /// TODO switch?
    auto rightInputSchema = join.getInputSchemas()[0];
    auto leftInputSchema = join.getInputSchemas()[1];
    auto outputSchema = join.getOutputSchema();
    auto outputOriginId = join.getOutputOriginIds()[0];
    auto logicalJoinFunction = join.getJoinFunction();
    auto windowType = NES::Util::as<Windowing::TimeBasedWindowType>(join.getWindowType());
    const auto pageSize = NES::Configurations::DEFAULT_PAGED_VECTOR_SIZE;


    auto nested = logicalOperator.getInputOriginIds();
    auto flat_view = nested | std::views::join;
    std::vector inputOriginIds(flat_view.begin(), flat_view.end());

    auto joinFunction = QueryCompilation::FunctionProvider::lowerFunction(logicalJoinFunction);
    auto leftMemoryProvider = TupleBufferMemoryProvider::create(pageSize, leftInputSchema);
    leftMemoryProvider->getMemoryLayout()->setKeyFieldNames(getJoinFieldNames(leftInputSchema, logicalJoinFunction));
    auto rightMemoryProvider = TupleBufferMemoryProvider::create(pageSize, rightInputSchema);
    rightMemoryProvider->getMemoryLayout()->setKeyFieldNames(getJoinFieldNames(rightInputSchema, logicalJoinFunction));

    /// TODO switch
    auto [timeStampFieldRight, timeStampFieldLeft] = getTimestampLeftAndRight(join, windowType);

    auto leftBuildOperator
        = NLJBuildPhysicalOperator(leftMemoryProvider, handlerId, JoinBuildSideType::Left, timeStampFieldLeft.toTimeFunction());

    auto rightBuildOperator
        = NLJBuildPhysicalOperator(rightMemoryProvider, handlerId, JoinBuildSideType::Right, timeStampFieldRight.toTimeFunction());

    auto joinSchema = JoinSchema(leftInputSchema, rightInputSchema, outputSchema);
    auto probeOperator = NLJProbePhysicalOperator(
        handlerId,
        joinFunction,
        join.getWindowStartFieldName(),
        join.getWindowEndFieldName(),
        joinSchema,
        leftMemoryProvider,
        rightMemoryProvider);

    constexpr uint64_t numberOfOriginIds = 2;
    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(
        windowType->getSize().getTime(), windowType->getSlide().getTime(), numberOfOriginIds);
    auto handler = std::make_shared<NLJOperatorHandler>(
        inputOriginIds, outputOriginId, std::move(sliceAndWindowStore), leftMemoryProvider, rightMemoryProvider);

    auto leftBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(leftBuildOperator, leftInputSchema, outputSchema);
    leftBuildWrapper->handlerId = handlerId;
    leftBuildWrapper->handler = handler;
    leftBuildWrapper->isEmit = true;

    auto rightBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(rightBuildOperator, rightInputSchema, outputSchema);
    rightBuildWrapper->handlerId = handlerId;
    rightBuildWrapper->handler = handler;
    rightBuildWrapper->isEmit = true;

    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(probeOperator, outputSchema, outputSchema);
    probeWrapper->handlerId = handlerId;
    probeWrapper->handler = handler;
    probeWrapper->isScan = true;

    probeWrapper->children.push_back(leftBuildWrapper);
    probeWrapper->children.push_back(rightBuildWrapper);

    return {{probeWrapper}, {rightBuildWrapper, leftBuildWrapper}};
};

std::unique_ptr<AbstractRewriteRule> RewriteRuleGeneratedRegistrar::RegisterJoinRewriteRule(RewriteRuleRegistryArguments argument)
{
    return std::make_unique<LowerToPhysicalNLJoin>(argument.conf);
}

}
