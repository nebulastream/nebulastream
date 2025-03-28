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
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/Operator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalNLJoin.hpp>
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
                return std::make_unique<EventTimeFunction>(
                    std::make_unique<::NES::Functions::FieldAccessPhysicalFunction>(fieldName), unit);
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


std::tuple<TimestampField, TimestampField>
getTimestampLeftAndRight(JoinLogicalOperator joinOperator, const std::shared_ptr<Windowing::TimeBasedWindowType>& windowType)
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
        auto timeStampFieldNameLeft = findTimeStampFieldName(joinOperator.getLeftInputSchema());
        auto timeStampFieldNameRight = findTimeStampFieldName(joinOperator.getRightInputSchema());

        INVARIANT(
            !(timeStampFieldNameLeft.empty() || timeStampFieldNameRight.empty()),
            "Could not find timestampfieldname {} in both streams!",
            timeStampFieldNameWithoutSourceName);

        return {
            TimestampField::EventTime(timeStampFieldNameLeft, windowType->getTimeCharacteristic()->getTimeUnit()),
            TimestampField::EventTime(timeStampFieldNameRight, windowType->getTimeCharacteristic()->getTimeUnit())};
    }
}

/// TODO we want to return here all three operators
std::vector<std::shared_ptr<PhysicalOperator>> LowerToPhysicalNLJoin::applyToPhysical(DynamicTraitSet<QueryForSubtree, Operator>* traitSet)
{
    const auto operatorHandlerIndex = 0; // TODO this should change. In the best case we have setIndex() for all the operators.

    const auto op = traitSet->get<Operator>();
    const auto ops = dynamic_cast<JoinLogicalOperator*>(op);
    const auto outSchema = ops->getOutputSchema();
    const std::shared_ptr<Functions::PhysicalFunction> joinFunction
        = ::NES::QueryCompilation::FunctionProvider::lowerFunction(ops->getJoinFunction());

    const auto leftSchema = ops->getLeftInputSchema();
    const auto rightSchema = ops->getRightInputSchema();
    const auto outputSchema = ops->getOutputSchema();

    auto leftMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(conf.pageSize.getValue(), leftSchema);
    auto rightMemoryProvider
        = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(conf.pageSize.getValue(), rightSchema);
    auto probeMemoryProvider
        = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(conf.pageSize.getValue(), outputSchema);

    const auto windowType = NES::Util::as<Windowing::TimeBasedWindowType>(ops->getWindowType());
    const auto [timeStampFieldLeft, timeStampFieldRight] = getTimestampLeftAndRight(*ops, windowType);

    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(ops->getLeftInputSchema(), conf.bufferSize.getValue());
    auto memoryProvider1 = std::make_unique<RowTupleBufferMemoryProvider>(layout);
    auto leftBuildOp = std::make_shared<NLJBuildPhysicalOperator>(
        (
            [](auto ptr)
            {
                std::vector<std::shared_ptr<TupleBufferMemoryProvider>> vec;
                vec.push_back(std::move(ptr));
                return vec;
            })(std::move(memoryProvider1)),
        operatorHandlerIndex,
        JoinBuildSideType::Left,
        timeStampFieldLeft.toTimeFunction(),
        leftMemoryProvider);

    auto memoryProvider2 = std::make_unique<RowTupleBufferMemoryProvider>(layout);
    auto rightBuildOp = NLJBuildPhysicalOperator(
        {std::move(memoryProvider2)},
        operatorHandlerIndex,
        JoinBuildSideType::Right,
        timeStampFieldRight.toTimeFunction(),
        rightMemoryProvider);

    auto joinSchema = JoinSchema(leftSchema, rightSchema, outputSchema);
    auto memoryProvider3 = std::make_unique<RowTupleBufferMemoryProvider>(layout);
    auto probeOp = new NLJProbePhysicalOperator(
        operatorHandlerIndex,
        joinFunction,
        ops->getWindowStartFieldName(),
        ops->getWindowEndFieldName(),
        joinSchema,
        leftMemoryProvider,
        rightMemoryProvider);

    constexpr uint64_t numberOfOriginIds = 2;
    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(
        windowType->getSize().getTime(), windowType->getSlide().getTime(), numberOfOriginIds);

    auto handler = std::make_unique<NLJOperatorHandler>(
        ops->getAllInputOriginIds(), ops->getOutputOriginIds()[0], std::move(sliceAndWindowStore), leftMemoryProvider, rightMemoryProvider);
    return {probeOp, rightBuildOp, leftBuildOp};
};

std::unique_ptr<AbstractRewriteRule> RewriteRuleGeneratedRegistrar::RegisterJoinRewriteRule(RewriteRuleRegistryArguments argument)
{
    return std::make_unique<LowerToPhysicalNLJoin>(argument.conf);
}

}
