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

#pragma once

#include <memory>
#include <set>
#include <tuple>
#include <TraitSets/TraitSet.hpp>
#include <TraitSets/Traits/Children.hpp>
#include <TraitSets/Traits/QueryForSubtree.hpp>
#include <TraitSets/RewriteRules/AbstractRewriteRule.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJProbe.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJBuild.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Functions/FunctionProvider.hpp>
#include <OptimizerConfiguration.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <Streaming/Join/StreamJoinUtil.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>

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
    [[nodiscard]] Windowing::TimeUnit getUnit() const
    {
        return unit;
    }

    [[nodiscard]] const std::string& getName() const
    {
        return fieldName;
    }

    [[nodiscard]] const TimeFunctionType& getTimeFunctionType() const
    {
        return timeFunctionType;
    }

    /// Builds the TimeFunction
    [[nodiscard]] std::shared_ptr<TimeFunction> toTimeFunction() const
    {
        switch (timeFunctionType)
        {
            case EVENT_TIME:
                return std::make_unique<EventTimeFunction>(
                    std::make_unique<::NES::Functions::ReadFieldPhysicalFunction>(fieldName), unit);
            case INGESTION_TIME:
                return std::make_unique<IngestionTimeFunction>();
        }
    }
    static TimestampField IngestionTime();
    static TimestampField EventTime(std::string fieldName, Windowing::TimeUnit tm);

private:
    std::string fieldName;
    Windowing::TimeUnit unit;
    TimeFunctionType timeFunctionType;
    TimestampField(std::string fieldName, Windowing::TimeUnit unit, TimeFunctionType timeFunctionType);
};


std::tuple<TimestampField, TimestampField> getTimestampLeftAndRight(
    const std::shared_ptr<JoinLogicalOperator>& joinOperator, const std::shared_ptr<Windowing::TimeBasedWindowType>& windowType)
{
    if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
    {
        NES_DEBUG("Skip eventime identification as we use ingestion time");
        return {TimestampField::IngestionTime(), TimestampField::IngestionTime()};
    }
    else
    {
        /// FIXME Once #3407 is done, we can change this to get the left and right fieldname
        auto timeStampFieldName = windowType->getTimeCharacteristic()->getField()->getName();
        auto timeStampFieldNameWithoutSourceName = timeStampFieldName.substr(timeStampFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR));

        /// Lambda function for extracting the timestamp from a schema
        auto findTimeStampFieldName = [&](const std::shared_ptr<Schema>& schema)
        {
            for (const auto& field : *schema)
            {
                if (field->getName().find(timeStampFieldNameWithoutSourceName) != std::string::npos)
                {
                    return field->getName();
                }
            }
            return std::string();
        };

        /// Extracting the left and right timestamp
        auto timeStampFieldNameLeft = findTimeStampFieldName(joinOperator->getLeftInputSchema());
        auto timeStampFieldNameRight = findTimeStampFieldName(joinOperator->getRightInputSchema());

        INVARIANT(
            !(timeStampFieldNameLeft.empty() || timeStampFieldNameRight.empty()),
            "Could not find timestampfieldname {} in both streams!",
            timeStampFieldNameWithoutSourceName);

        return {
            TimestampField::EventTime(timeStampFieldNameLeft, windowType->getTimeCharacteristic()->getTimeUnit()),
            TimestampField::EventTime(timeStampFieldNameRight, windowType->getTimeCharacteristic()->getTimeUnit())};
    }
}


/// I guess this should be another type
struct LowerToPhysicalNLJoin : TypedAbstractRewriteRule<QueryForSubtree, Operator>
{
    LowerToPhysicalNLJoin(std::shared_ptr<OptimizerConfiguration> conf) : conf(conf) {};

    std::shared_ptr<OptimizerConfiguration> conf;

    /// TODO we want to return here all three operators
    DynamicTraitSet<QueryForSubtree, Operator>* applyTyped(DynamicTraitSet<QueryForSubtree, Operator>* traitSet) override
    {
        const auto operatorHandlerIndex = 0; // TODO this should change. In the best case we have setIndex() for all the operators.

        const auto ops = traitSet->get<JoinLogicalOperator>();
        const auto outSchema = ops->getOutputSchema();
        const auto joinFunction = ::NES::QueryCompilation::FunctionProvider::lowerFunction(ops->getJoinFunction());

        const auto leftSchema = ops->getLeftInputSchema();
        const auto rightSchema = ops->getRightInputSchema();
        const auto outputSchema = ops->getOutputSchema();

        auto leftMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
            conf->joinOptions.pageSize, leftSchema);
        auto rightMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
            conf->joinOptions.pageSize, rightSchema);
        auto probeMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
            conf->joinOptions.pageSize, outputSchema);

        const auto windowType = NES::Util::as<Windowing::TimeBasedWindowType>(ops->getJoinDefinition()->getWindowType());
        const auto [timeStampFieldLeft, timeStampFieldRight] = getTimestampLeftAndRight(ops, windowType);

        const auto metaData = WindowMetaData(ops->getWindowStartFieldName(), ops->getWindowEndFieldName());

        auto leftBuildOp = std::make_unique<NLJBuild>(
            operatorHandlerIndex,
            QueryCompilation::JoinBuildSideType::Left,
            timeStampFieldLeft,
            leftMemoryProvider);

        auto rightBuildOp = std::make_unique<NLJBuild>(
            operatorHandlerIndex,
            QueryCompilation::JoinBuildSideType::Right,
            timeStampFieldRight,
            rightMemoryProvider);

        auto probeOp = std::make_unique<NLJProbe>(
                operatorHandlerIndex,
                joinFunction,
                metaData,
                outputSchema,
                leftMemoryProvider,
                rightMemoryProvider);

        constexpr uint64_t numberOfOriginIds = 2;
        auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(windowType->getSize(), windowType->getSlide(), numberOfOriginIds);

        auto handler = std::make_unique<NLJOperatorHandler>(
            ops->getAllInputOriginIds(),
            ops->getOutputOriginIds()[0],
            std::move(sliceAndWindowStore),
            leftMemoryProvider,
            rightMemoryProvider);
        return traitSet;
    };
};

}
