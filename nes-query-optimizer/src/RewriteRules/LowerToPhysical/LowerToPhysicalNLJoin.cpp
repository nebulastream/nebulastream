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

#include <memory>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalNLJoin.hpp>
#include <RewriteRuleRegistry.hpp>
#include <cstdint>
#include <memory>
#include <tuple>
#include <utility>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJBuildPhysicalOperator.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Streaming/Join/NestedLoopJoin/NLJProbePhysicalOperator.hpp>
#include <Streaming/Join/StreamJoinUtil.hpp>
#include <Util/Common.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
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
    [[nodiscard]] std::unique_ptr<TimeFunction> toTimeFunction() const
    {
        switch (timeFunctionType)
        {
            case EVENT_TIME:
                return std::make_unique<EventTimeFunction>(
                    std::make_unique<::NES::Functions::FieldAccessPhysicalFunction>(fieldName), unit);
            case INGESTION_TIME:
                return std::make_unique<IngestionTimeFunction>();
        }
    }
    static TimestampField IngestionTime()
    {
        return {"IngestionTime", Windowing::TimeUnit(1), INGESTION_TIME};
    }
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


std::tuple<TimestampField, TimestampField> getTimestampLeftAndRight(const JoinLogicalOperator& joinOperator, const Windowing::TimeBasedWindowType& windowType)
{
    if (windowType.getTimeCharacteristic().getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
    {
        NES_DEBUG("Skip eventime identification as we use ingestion time");
        return {TimestampField::IngestionTime(), TimestampField::IngestionTime()};
    }
    else
    {
        /// FIXME Once #3407 is done, we can change this to get the left and right fieldname
        auto timeStampFieldName = windowType.getTimeCharacteristic().getField().getName();
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
        auto timeStampFieldNameLeft = findTimeStampFieldName(joinOperator.getLeftInputSchema());
        auto timeStampFieldNameRight = findTimeStampFieldName(joinOperator.getRightInputSchema());

        INVARIANT(
            !(timeStampFieldNameLeft.empty() || timeStampFieldNameRight.empty()),
            "Could not find timestampfieldname {} in both streams!",
            timeStampFieldNameWithoutSourceName);

        return {
            TimestampField::EventTime(timeStampFieldNameLeft, windowType.getTimeCharacteristic().getTimeUnit()),
            TimestampField::EventTime(timeStampFieldNameRight, windowType.getTimeCharacteristic().getTimeUnit())};
    }
}

std::vector<PhysicalOperatorWrapper> LowerToPhysicalNLJoin::apply(JoinLogicalOperator& queryPlan)
{
    const auto operatorHandlerIndex = 0; // TODO this should change. In the best case we have setIndex() for all the operators.

    auto* op = traitSet->get<Operator>();

    auto ops = dynamic_cast<JoinLogicalOperator*>(op);
    auto outSchema = ops->getOutputSchema();
    auto joinFunction = ::NES::QueryCompilation::FunctionProvider::lowerFunction(ops->getJoinFunction().clone());

    auto leftMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
        conf.pageSize.getValue(), ops->getLeftInputSchema());
    auto rightMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
        conf.pageSize.getValue(), ops->getRightInputSchema());
    auto probeMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
        conf.pageSize.getValue(), ops->getOutputSchema());

    auto windowType = dynamic_cast<Windowing::TimeBasedWindowType*>(&ops->getWindowType());
    auto [timeStampFieldLeft, timeStampFieldRight] = getTimestampLeftAndRight(*ops, *windowType);

    auto leftBuildOp = std::make_unique<NLJBuildPhysicalOperator>(
        std::move(leftMemoryProvider),
        operatorHandlerIndex,
        JoinBuildSideType::Left,
        timeStampFieldLeft.toTimeFunction());

    auto rightBuildOp = std::make_unique<NLJBuildPhysicalOperator>(
        std::move(rightMemoryProvider),
        operatorHandlerIndex,
        JoinBuildSideType::Right,
        timeStampFieldRight.toTimeFunction());

    auto leftMemoryProvider2 = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
        conf.pageSize.getValue(), ops->getLeftInputSchema());
    auto rightMemoryProvider2 = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
        conf.pageSize.getValue(), ops->getRightInputSchema());
    auto joinSchema = JoinSchema(ops->getLeftInputSchema(), ops->getRightInputSchema(), ops->getOutputSchema());
    auto probeOp = std::make_unique<NLJProbePhysicalOperator>(
        operatorHandlerIndex,
        std::move(joinFunction),
        ops->getWindowStartFieldName(),
        ops->getWindowEndFieldName(),
        joinSchema,
        std::move(leftMemoryProvider2),
        std::move(rightMemoryProvider2));

    constexpr uint64_t numberOfOriginIds = 2;
    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(windowType->getSize().getTime(), windowType->getSlide().getTime(), numberOfOriginIds);
    auto leftMemoryProvider3 = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
        conf.pageSize.getValue(), ops->getLeftSchema());
    auto rightMemoryProvider3 = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
        conf.pageSize.getValue(), ops->getRightSchema());
    auto handler = std::make_unique<NLJOperatorHandler>(
        ops->getAllInputOriginIds(),
        ops->getOutputOriginIds()[0],
        std::move(sliceAndWindowStore),
        std::move(leftMemoryProvider3),
        std::move(rightMemoryProvider3));

    auto probeOpWrapper = PhysicalOperatorWrapper{std::move(probeOp), ops->getOutputSchema(), ops->getOutputSchema(), nullptr};
    auto rightBuildOpWrapper = PhysicalOperatorWrapper{std::move(rightBuildOp), ops->getRightSchema(), ops->getOutputSchema(), nullptr};
    auto leftBuildOpWrapper = PhysicalOperatorWrapper{std::move(leftBuildOp), ops->getLeftSchema(), ops->getOutputSchema(), nullptr};

    std::vector<PhysicalOperatorWrapper> physicalOperatorVec;
    physicalOperatorVec.emplace_back(std::move(probeOpWrapper));
    physicalOperatorVec.emplace_back(std::move(rightBuildOpWrapper));
    physicalOperatorVec.emplace_back(std::move(leftBuildOpWrapper));

    return physicalOperatorVec;
};

std::unique_ptr<AbstractRewriteRule> RewriteRuleGeneratedRegistrar::RegisterJoinRewriteRule(RewriteRuleRegistryArguments argument)
{
    return std::make_unique<LowerToPhysicalNLJoin>(argument.conf);
}

}
