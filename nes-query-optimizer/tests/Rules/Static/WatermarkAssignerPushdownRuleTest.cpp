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

#include <array>
#include <utility>
#include <vector>
#include <Rules/Static/WatermarkAssignerPushdownRule.hpp>


#include <DataTypes/DataType.hpp>
#include <Functions/ArithmeticalFunctions/MulLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class WatermarkAssignerPushdownRuleTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("WatermarkAssignerPushdownRuleTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
};

TEST_F(WatermarkAssignerPushdownRuleTest, NonBreakerOperators)
{
    /// BEFORE: Sink < EventTime < IngestionTime < Selection < Union < (source1, source2)
    /// AFTER:  Sink < Selection < Union < (eventTime < ingestionTime < source1), (eventTime < ingestionTime < source2)


    auto source1 = utils.createSource("nonbreakers1", {"a"});
    auto source2 = utils.createSource("nonbreakers2", {"a"});

    auto unionOp = UnionLogicalOperator::create(std::vector<LogicalOperator>{source1, source2});

    auto select = SelectionLogicalOperator::create(
        unionOp,
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{unionOp.getOutputSchema()[Identifier::parse("a")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}});

    auto ingestionTime = IngestionTimeWatermarkAssignerLogicalOperator::create(select);
    auto eventTime = EventTimeWatermarkAssignerLogicalOperator::create(
        ingestionTime, FieldAccessLogicalFunction{ingestionTime.getOutputSchema()[Identifier::parse("a")].value()}, Windowing::TimeUnit{0});

    auto sink = utils.createSink(eventTime, "nonbreakers", {"a"});
    auto plan = utils.createPlan(sink);

    auto pushed = WatermarkAssignerPushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());

    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<UnionLogicalOperator>());

    auto leftOp0 = op2.getChildren().at(0);
    ASSERT_TRUE(leftOp0.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>());
    auto leftOp1 = leftOp0.getChildren().at(0);
    ASSERT_TRUE(leftOp1.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>());
    auto leftOp2 = leftOp1.getChildren().at(0);
    ASSERT_TRUE(leftOp2.tryGetAs<SourceDescriptorLogicalOperator>());

    auto rightOp0 = op2.getChildren().at(1);
    ASSERT_TRUE(rightOp0.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>());
    auto rightOp1 = rightOp0.getChildren().at(0);
    ASSERT_TRUE(rightOp1.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>());
    auto rightOp2 = rightOp1.getChildren().at(0);
    ASSERT_TRUE(rightOp2.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(WatermarkAssignerPushdownRuleTest, ProjectionBreaker)
{
    /// BEFORE: Sink < EventTime < IngestionTime < Projection < source
    /// AFTER: Sink < EventTime < Projection < IngestionTime < source

    auto source = utils.createSource("projectionBreaker", {"a", "b"});
    auto projection = ProjectionLogicalOperator::create(
        source,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("c"),
             MulLogicalFunction{
                 FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
                 ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}}}},
        ProjectionLogicalOperator::Asterisk{false});

    auto ingestionTime = IngestionTimeWatermarkAssignerLogicalOperator::create(projection);
    auto eventTime = EventTimeWatermarkAssignerLogicalOperator::create(
        ingestionTime, FieldAccessLogicalFunction{ingestionTime.getOutputSchema()[Identifier::parse("c")].value()}, Windowing::TimeUnit{0});

    auto sink = utils.createSink(eventTime, "projectionBreakers", {"c"});

    auto plan = utils.createPlan(sink);

    auto pushed = WatermarkAssignerPushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<ProjectionLogicalOperator>());
    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>());
    auto op4 = op3.getChildren().at(0);
    ASSERT_TRUE(op4.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(WatermarkAssignerPushdownRuleTest, ProjectionNonBreaker)
{
    /// BEFORE Sink < EventTime < IngestionTime < Projection < source
    /// AFTER Sink < Projection < EventTime < IngestionTime < source

    auto source = utils.createSource("projectionNonBreaker", {"a", "b"});
    auto projection = ProjectionLogicalOperator::create(
        source,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("c"),
             MulLogicalFunction{
                 FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
                 ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}}},
            {Identifier::parse("a"), FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()}}},
        ProjectionLogicalOperator::Asterisk{false});

    auto ingestionTime = IngestionTimeWatermarkAssignerLogicalOperator::create(projection);
    auto eventTime = EventTimeWatermarkAssignerLogicalOperator::create(
        ingestionTime, FieldAccessLogicalFunction{ingestionTime.getOutputSchema()[Identifier::parse("a")].value()}, Windowing::TimeUnit{0});

    auto sink = utils.createSink(eventTime, "projectionNonBreakers", {"a", "c"});

    auto plan = utils.createPlan(sink);

    auto pushed = WatermarkAssignerPushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<ProjectionLogicalOperator>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>());
    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>());
    auto op4 = op3.getChildren().at(0);
    ASSERT_TRUE(op4.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(WatermarkAssignerPushdownRuleTest, ProjectionAsteriskNonBreaker)
{
    /// BEFORE Sink < EventTime < IngestionTime < Projection < source
    /// AFTER Sink < Projection < EventTime < IngestionTime < source

    auto source = utils.createSource("projectionAsteriskNonBreaker", {"a", "b"});
    auto projection = ProjectionLogicalOperator::create(source, {}, ProjectionLogicalOperator::Asterisk{true});

    auto ingestionTime = IngestionTimeWatermarkAssignerLogicalOperator::create(projection);
    auto eventTime = EventTimeWatermarkAssignerLogicalOperator::create(
        ingestionTime, FieldAccessLogicalFunction{ingestionTime.getOutputSchema()[Identifier::parse("a")].value()}, Windowing::TimeUnit{0});

    auto sink = utils.createSink(eventTime, "projectionAsteriskNonBreakers", {"a", "b"});

    auto plan = utils.createPlan(sink);

    auto pushed = WatermarkAssignerPushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<ProjectionLogicalOperator>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>());
    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>());
    auto op4 = op3.getChildren().at(0);
    ASSERT_TRUE(op4.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(WatermarkAssignerPushdownRuleTest, ProjectionRenameBreaker)
{
    /// BEFORE: Sink < EventTime < IngestionTime < Projection < source
    /// AFTER: Sink < EventTime < Projection < IngestionTime < source

    auto source = utils.createSource("projectionRenameBreaker", {"a", "b"});
    auto projection = ProjectionLogicalOperator::create(
        source,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("c"), FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()}}},
        ProjectionLogicalOperator::Asterisk{false});

    auto ingestionTime = IngestionTimeWatermarkAssignerLogicalOperator::create(projection);
    auto eventTime = EventTimeWatermarkAssignerLogicalOperator::create(
        ingestionTime, FieldAccessLogicalFunction{ingestionTime.getOutputSchema()[Identifier::parse("c")].value()}, Windowing::TimeUnit{0});

    auto sink = utils.createSink(eventTime, "projectionRenameBreaker", {"c"});

    auto plan = utils.createPlan(sink);

    auto pushed = WatermarkAssignerPushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<ProjectionLogicalOperator>());
    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>());
    auto op4 = op3.getChildren().at(0);
    ASSERT_TRUE(op4.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(WatermarkAssignerPushdownRuleTest, ProjectionPartialBreakers)
{
    /// BEFORE: sink < EventTime(b) < EventTime(c) < projection(b, c <- a*2) < source
    /// AFTER: sink < EventTime(c) < projection(b, c <- a*2) < EventTime(b) < source

    auto source = utils.createSource("projectionPartialBreaker", {"a", "b"});
    auto projection = ProjectionLogicalOperator::create(
        source,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("c"),
             MulLogicalFunction{
                 FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
                 ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}}},
            {Identifier::parse("b"), FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("b")].value()}}},
        ProjectionLogicalOperator::Asterisk{false});

    auto eventTime1 = EventTimeWatermarkAssignerLogicalOperator::create(
        projection, FieldAccessLogicalFunction{projection.getOutputSchema()[Identifier::parse("c")].value()}, Windowing::TimeUnit{1});
    auto eventTime2 = EventTimeWatermarkAssignerLogicalOperator::create(
        eventTime1, FieldAccessLogicalFunction{eventTime1.getOutputSchema()[Identifier::parse("b")].value()}, Windowing::TimeUnit{2});

    auto sink = utils.createSink(eventTime2, "projectionPartialBreakers", {"c", "b"});

    auto plan = utils.createPlan(sink);

    auto pushed = WatermarkAssignerPushdownRule{}.apply(plan);


    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>());
    ASSERT_EQ(op1.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>().value()->getUnit(), Windowing::TimeUnit{1});
    ASSERT_TRUE(
        op1.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>()
            .value()
            ->getOnField()
            .tryGetAs<FieldAccessLogicalFunction>()
            .value()
            ->getField()
            .getLastName()
        == Identifier::parse("c"));
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<ProjectionLogicalOperator>());
    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>());
    ASSERT_EQ(op3.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>().value()->getUnit(), Windowing::TimeUnit{2});
    ASSERT_TRUE(
        op3.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>()
            .value()
            ->getOnField()
            .tryGetAs<FieldAccessLogicalFunction>()
            .value()
            ->getField()
            .getLastName()
        == Identifier::parse("b"));
    auto op4 = op3.getChildren().at(0);
    ASSERT_TRUE(op4.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(WatermarkAssignerPushdownRuleTest, Breakers)
{
    /// BEFORE AND AFTER: Sink < IngestionTime < Join < Source

    auto sourceLeft = utils.createSource("breakers1", {"a", "b"});
    auto sourceRight = utils.createSource("breakers2", {"c", "d"});

    auto joinFunction = EqualsLogicalFunction{
        FieldAccessLogicalFunction{sourceLeft.getOutputSchema()[Identifier::parse("b")].value()},
        FieldAccessLogicalFunction{sourceRight.getOutputSchema()[Identifier::parse("c")].value()}};
    auto join = JoinLogicalOperator::create(
        std::array<LogicalOperator, 2>{sourceLeft, sourceRight},
        joinFunction,
        Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{0}}},
        JoinLogicalOperator::JoinType{},
        JoinTimeCharacteristic{});

    auto ingestionTime = IngestionTimeWatermarkAssignerLogicalOperator::create(join);

    auto sink = utils.createSink(ingestionTime, "breakers", {"start", "end", "a", "b", "c", "d"});

    auto plan = utils.createPlan(sink);

    auto pushed = WatermarkAssignerPushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<JoinLogicalOperator>());
    auto leftOp0 = op2.getChildren().at(0);
    ASSERT_TRUE(leftOp0.tryGetAs<SourceDescriptorLogicalOperator>());
    auto rightOp0 = op2.getChildren().at(1);
    ASSERT_TRUE(rightOp0.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(WatermarkAssignerPushdownRuleTest, WithoutWatermarkAssigner)
{
    /// Ensures plans without watermark assigners pass unchanged

    auto source = utils.createSource("noopt", {"a", "b", "c"});
    auto sink = utils.createSink(source, "noopt", {"a", "b", "c"});
    auto plan = utils.createPlan(sink);

    auto pushed = WatermarkAssignerPushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(WatermarkAssignerPushdownRuleTest, ProjectionTwoApplyNowOrder)
{
    /// BEFORE: sink < ET(d, unit=2) < ET(c, unit=1) < projection(c<-a*2, d<-b*2) < source(a, b)
    /// EXPECTED: topmost WA below sink is ET(d).
    auto source = utils.createSource("twoApplyNow", {"a", "b"});
    auto projection = TypedLogicalOperator<ProjectionLogicalOperator>{
        source,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("c"),
             MulLogicalFunction{
                 FieldAccessLogicalFunction{source.getOutputSchema().getFieldByName(Identifier::parse("a")).value()},
                 ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}}},
            {Identifier::parse("d"),
             MulLogicalFunction{
                 FieldAccessLogicalFunction{source.getOutputSchema().getFieldByName(Identifier::parse("b")).value()},
                 ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}}}},
        ProjectionLogicalOperator::Asterisk{false}};
    auto etC = TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>{
        projection,
        FieldAccessLogicalFunction{projection.getOutputSchema().getFieldByName(Identifier::parse("c")).value()},
        Windowing::TimeUnit{1}};
    auto etD = TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>{
        etC, FieldAccessLogicalFunction{etC.getOutputSchema().getFieldByName(Identifier::parse("d")).value()}, Windowing::TimeUnit{2}};
    auto sink = utils.createSink(etD, "twoApplyNow", {"c", "d"});
    auto pushed = WatermarkAssignerPushdownRule{}.apply(utils.createPlan(sink));

    auto topWA = pushed.getRootOperators().at(0).getChildren().at(0);
    ASSERT_EQ(
        topWA.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>()
            .value()
            ->getOnField()
            .tryGetAs<FieldAccessLogicalFunction>()
            .value()
            ->getField()
            .getLastName(),
        Identifier::parse("d"));
}

TEST_F(WatermarkAssignerPushdownRuleTest, ProjectionAsteriskWithComputedFieldBreaker)
{
    /// BEFORE: sink < ET(c) < projection(Asterisk{true}, c <- a*2) < source(a, b)
    /// EXPECTED: ET(c) stays above projection (c is computed, not pushable)
    auto source = utils.createSource("projectionAsteriskComputed", {"a", "b"});
    auto projection = ProjectionLogicalOperator::create(
        source,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("c"),
             MulLogicalFunction{
                 FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
                 ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "2"}}}},
        ProjectionLogicalOperator::Asterisk{true});

    auto eventTime = EventTimeWatermarkAssignerLogicalOperator::create(
        projection, FieldAccessLogicalFunction{projection.getOutputSchema()[Identifier::parse("c")].value()}, Windowing::TimeUnit{0});
    auto sink = utils.createSink(eventTime, "projectionAsteriskComputed", {"a", "b", "c"});
    auto plan = utils.createPlan(sink);

    auto pushed = WatermarkAssignerPushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<ProjectionLogicalOperator>());
    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<SourceDescriptorLogicalOperator>());
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
