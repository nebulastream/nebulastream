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
#include <optional>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Static/ProjectionPushdownRule.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class ProjectionPushdownRuleTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ProjectionPushdownTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
};

TEST_F(ProjectionPushdownRuleTest, ValidatePushBeyondSelection)
{
    /// PLAN BEFORE: Sink(a,b) < PROJECT(a,b) < SELECT(c > 0) < Source(a,b,c,d)
    /// PLAN AFTER: Sink(a,b) < PROJECT(a,b) < SELECT(c > 0) < Project(a,b,c) < Source(a,b,c,d)

    auto source = utils.createSource("selection", {"a", "b", "c", "d"});


    auto predicate = EqualsLogicalFunction{
        FieldAccessLogicalFunction{
            Field{source, Identifier::parse("c"), DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}}},
        ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "0"}};
    auto selection = SelectionLogicalOperator::create(source, predicate);

    const std::vector<std::pair<Identifier, LogicalFunction>> projections{
        {Identifier::parse("a"), FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()}},
        {Identifier::parse("b"), FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("b")].value()}}};

    auto projection = ProjectionLogicalOperator::create(selection, projections, ProjectionLogicalOperator::Asterisk{false});


    auto sink = utils.createSink(projection, "selection", utils.createSchema({"a", "b"}));
    auto plan = utils.createPlan(sink);

    auto pushed = ProjectionPushdownRule{}.apply(plan);


    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(op1.getOutputSchema().size(), 2);
    ASSERT_TRUE(op1.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(op1.getOutputSchema()[Identifier::parse("b")]);
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SelectionLogicalOperator>());
    ASSERT_EQ(op2.getOutputSchema().size(), 3);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("b")]);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("c")]);
    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(op3.getOutputSchema().size(), 3);
    ASSERT_TRUE(op3.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(op3.getOutputSchema()[Identifier::parse("b")]);
    ASSERT_TRUE(op3.getOutputSchema()[Identifier::parse("c")]);

    auto op4 = op3.getChildren().at(0);
    ASSERT_TRUE(op4.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_EQ(op4.getOutputSchema().size(), 4);
}

TEST_F(ProjectionPushdownRuleTest, ValidatePushBeyondProjection)
{
    /// PLAN BEFORE: Sink(a, x) < Projection(a, b -> x) < Projection(a,b,c) < Source (a,b,c,d)
    /// PLAN AFTER: Sink(a, x) < Projection(a, b -> x) < Projection(a,b) < Projection(a,b) < Source (a,b,c,d)

    auto source = utils.createSource("projection", {"a", "b", "c", "d"});

    auto projections1 = std::vector<std::pair<Identifier, LogicalFunction>>{
        {Identifier::parse("a"), FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()}},
        {Identifier::parse("b"), FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("b")].value()}},
        {Identifier::parse("c"), FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("c")].value()}}};
    auto projection1 = ProjectionLogicalOperator::create(source, projections1, ProjectionLogicalOperator::Asterisk{false});


    auto projections2 = std::vector<std::pair<Identifier, LogicalFunction>>{
        {Identifier::parse("a"), FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()}},
        {Identifier::parse("x"), FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("c")].value()}}};
    auto projection2 = ProjectionLogicalOperator::create(projection1, projections2, ProjectionLogicalOperator::Asterisk{false});

    auto sink = utils.createSink(projection2, "projection", utils.createSchema({"a", "x"}));
    auto plan = utils.createPlan(sink);

    auto pushed = ProjectionPushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(op1.getOutputSchema().size(), 2);
    ASSERT_TRUE(op1.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(op1.getOutputSchema()[Identifier::parse("x")]);
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(op2.getOutputSchema().size(), 2);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("c")]);
    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(op3.getOutputSchema().size(), 2);
    ASSERT_TRUE(op3.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(op3.getOutputSchema()[Identifier::parse("c")]);
    auto op4 = op3.getChildren().at(0);
    ASSERT_TRUE(op4.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_EQ(op4.getOutputSchema().size(), 4);
}

TEST_F(ProjectionPushdownRuleTest, ValidatePushBeyondJoin)
{
    /// PLAN BEFORE: sink(a) < projection(a) < join(b=c) < (source(a,b,x, ts2)),(source(c,d,y, ts1))
    /// PLAN AFTER: sink(a) < projection(a) < join(b=c) < (projection(a,b) < source(a,b,x)),(projection(c) < source(c,d,y))

    auto sourceLeft = utils.createSource("join1", {"a", "b", "x", "ts1"});
    auto sourceRight = utils.createSource("join2", {"c", "d", "y", "ts2"});


    auto joinFunction = EqualsLogicalFunction{
        FieldAccessLogicalFunction{sourceLeft.getOutputSchema()[Identifier::parse("b")].value()},
        FieldAccessLogicalFunction{sourceRight.getOutputSchema()[Identifier::parse("c")].value()}};

    auto join = JoinLogicalOperator::create(
        std::array<LogicalOperator, 2>{sourceLeft, sourceRight},
        joinFunction,
        Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{0}}},
        JoinLogicalOperator::JoinType{},
        JoinLogicalOperator::createJoinTimeCharacteristic(
            {Windowing::BoundTimeCharacteristic{
                 Windowing::BoundEventTimeCharacteristic{
                     .field = FieldAccessLogicalFunction{sourceLeft.getOutputSchema()[Identifier::parse("ts1")].value()}},
             },
             Windowing::BoundTimeCharacteristic{
                 Windowing::BoundEventTimeCharacteristic{
                     .field = FieldAccessLogicalFunction{sourceRight.getOutputSchema()[Identifier::parse("ts2")].value()}},
             }})
            .value());


    auto projection = ProjectionLogicalOperator::create(
        join,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("a"), FieldAccessLogicalFunction{join.getOutputSchema()[Identifier::parse("a")].value()}}},
        ProjectionLogicalOperator::Asterisk{false});

    auto sink = utils.createSink(projection, "join", utils.createSchema({"a"}));
    auto plan = utils.createPlan(sink);

    auto pushed = ProjectionPushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(op1.getOutputSchema().size(), 1);
    ASSERT_TRUE(op1.getOutputSchema()[Identifier::parse("a")]);

    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<JoinLogicalOperator>());
    ASSERT_EQ(op2.getOutputSchema().size(), 7);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("start")]);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("end")]);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("b")]);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("c")]);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("ts1")]);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("ts2")]);

    auto leftOp1 = op2.getChildren().at(0);
    ASSERT_TRUE(leftOp1.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(leftOp1.getOutputSchema().size(), 3);
    ASSERT_TRUE(leftOp1.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(leftOp1.getOutputSchema()[Identifier::parse("b")]);
    ASSERT_TRUE(leftOp1.getOutputSchema()[Identifier::parse("ts1")]);

    auto leftOp2 = leftOp1.getChildren().at(0);
    ASSERT_TRUE(leftOp2.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_EQ(leftOp2.getOutputSchema().size(), 4);
    ASSERT_TRUE(leftOp2.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(leftOp2.getOutputSchema()[Identifier::parse("b")]);
    ASSERT_TRUE(leftOp2.getOutputSchema()[Identifier::parse("x")]);
    ASSERT_TRUE(leftOp2.getOutputSchema()[Identifier::parse("ts1")]);

    auto rightOp1 = op2.getChildren().at(1);
    ASSERT_TRUE(rightOp1.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(rightOp1.getOutputSchema().size(), 2);
    ASSERT_TRUE(rightOp1.getOutputSchema()[Identifier::parse("c")]);
    ASSERT_TRUE(rightOp1.getOutputSchema()[Identifier::parse("ts2")]);

    auto rightOp2 = rightOp1.getChildren().at(0);
    ASSERT_TRUE(rightOp2.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_EQ(rightOp2.getOutputSchema().size(), 4);
    ASSERT_TRUE(rightOp2.getOutputSchema()[Identifier::parse("c")]);
    ASSERT_TRUE(rightOp2.getOutputSchema()[Identifier::parse("d")]);
    ASSERT_TRUE(rightOp2.getOutputSchema()[Identifier::parse("y")]);
    ASSERT_TRUE(rightOp2.getOutputSchema()[Identifier::parse("ts2")]);
}

TEST_F(ProjectionPushdownRuleTest, ValidatePushBeyondUnion)
{
    /// PLAN BEFORE: sink(a) < projection(a) < union(a,b) < (source(a,b), source(a,b))
    /// PLAN AFTER: sink(a) < projection(a) < union(a) < (projection(a) < source(a,b), projection(a) < source(a,b)

    auto sourceLeft = utils.createSource("unionLeft", {"a", "b"});
    auto sourceRight = utils.createSource("unionRight", {"a", "b"});

    const auto unionOp = UnionLogicalOperator::create(std::vector<LogicalOperator>{sourceLeft, sourceRight});

    const auto projection = ProjectionLogicalOperator::create(
        unionOp,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("a"), FieldAccessLogicalFunction{unionOp.getOutputSchema()[Identifier::parse("a")].value()}}},
        ProjectionLogicalOperator::Asterisk{false});

    auto sink = utils.createSink(projection, "union", {"a"});
    auto plan = utils.createPlan(sink);

    auto pushed = ProjectionPushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());

    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(op1.getOutputSchema().size(), 1);
    ASSERT_TRUE(op1.getOutputSchema()[Identifier::parse("a")]);

    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<UnionLogicalOperator>());
    ASSERT_EQ(op2.getOutputSchema().size(), 1);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("a")]);

    auto leftOp1 = op2.getChildren().at(0);
    ASSERT_TRUE(leftOp1.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(leftOp1.getOutputSchema().size(), 1);
    ASSERT_TRUE(leftOp1.getOutputSchema()[Identifier::parse("a")]);

    auto leftOp2 = leftOp1.getChildren().at(0);
    ASSERT_TRUE(leftOp2.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_EQ(leftOp2.getOutputSchema().size(), 2);
    ASSERT_TRUE(leftOp2.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(leftOp2.getOutputSchema()[Identifier::parse("b")]);

    auto rightOp1 = op2.getChildren().at(1);
    ASSERT_TRUE(rightOp1.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(rightOp1.getOutputSchema().size(), 1);
    ASSERT_TRUE(rightOp1.getOutputSchema()[Identifier::parse("a")]);

    auto rightOp2 = rightOp1.getChildren().at(0);
    ASSERT_TRUE(rightOp2.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_EQ(rightOp2.getOutputSchema().size(), 2);
    ASSERT_TRUE(rightOp2.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(rightOp2.getOutputSchema()[Identifier::parse("b")]);
}

TEST_F(ProjectionPushdownRuleTest, ValidatePushBeyondEventTimeWaterMarkAssigner)
{
    /// PLAN BEFORE: sink(a) < proj(a) < eventTimeWA(ts) < source(a,b,ts)
    /// PLAN AFTER: sink(a) < proj(a) < eventTimeWA(ts) < proj(a, ts) < source(a,b,ts)

    auto source = utils.createSource("eventTime", {"a", "b", "ts"});

    auto eventTimeWA = EventTimeWatermarkAssignerLogicalOperator::create(
        source, FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("ts")].value()}, Windowing::TimeUnit{0});


    const auto projection = ProjectionLogicalOperator::create(
        eventTimeWA,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("a"), FieldAccessLogicalFunction{eventTimeWA.getOutputSchema()[Identifier::parse("a")].value()}}},
        ProjectionLogicalOperator::Asterisk{false});

    auto sink = utils.createSink(projection, "eventTime", {"a"});
    auto plan = utils.createPlan(sink);

    auto pushed = ProjectionPushdownRule{}.apply(plan);


    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());

    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(op1.getOutputSchema().size(), 1);
    ASSERT_TRUE(op1.getOutputSchema()[Identifier::parse("a")]);

    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>());
    ASSERT_EQ(op2.getOutputSchema().size(), 2);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("ts")]);

    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(op3.getOutputSchema().size(), 2);
    ASSERT_TRUE(op3.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(op3.getOutputSchema()[Identifier::parse("ts")]);

    auto op4 = op3.getChildren().at(0);
    ASSERT_TRUE(op4.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_EQ(op4.getOutputSchema().size(), 3);
    ASSERT_TRUE(op4.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(op4.getOutputSchema()[Identifier::parse("b")]);
    ASSERT_TRUE(op4.getOutputSchema()[Identifier::parse("ts")]);
}

TEST_F(ProjectionPushdownRuleTest, ValidatePushBeyondIngestionTimeWaterMarkAssigner)
{
    /// PLAN BEFORE: sink(a) < proj(a) < ingestionTime() < source(a)
    /// PLAN AFTER: sink(a) < proj(a) < ingestionTime() < proj(a) < source(a)

    auto source = utils.createSource("ingestionTime", {"a", "b", "ts"});

    const auto ingestionTime = IngestionTimeWatermarkAssignerLogicalOperator::create(source);

    const auto projection = ProjectionLogicalOperator::create(
        ingestionTime,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("a"), FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()}}},
        ProjectionLogicalOperator::Asterisk{false});

    auto sink = utils.createSink(projection, "ingestionTime", {"a"});
    auto plan = utils.createPlan(sink);

    auto pushed = ProjectionPushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());

    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(op1.getOutputSchema().size(), 1);
    ASSERT_TRUE(op1.getOutputSchema()[Identifier::parse("a")]);

    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>());
    ASSERT_EQ(op2.getOutputSchema().size(), 1);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("a")]);

    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(op3.getOutputSchema().size(), 1);
    ASSERT_TRUE(op3.getOutputSchema()[Identifier::parse("a")]);

    auto op4 = op3.getChildren().at(0);
    ASSERT_TRUE(op4.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_EQ(op4.getOutputSchema().size(), 3);
    ASSERT_TRUE(op4.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(op4.getOutputSchema()[Identifier::parse("b")]);
    ASSERT_TRUE(op4.getOutputSchema()[Identifier::parse("ts")]);
}

TEST_F(ProjectionPushdownRuleTest, ValidatePushBeyondWindowedAggregation)
{
    /// PLAN BEFORE: sink(start, end, sum) < proj(start, end, sum), agg(sum(a) -> sum, c) < eventTime(ts) < source(a,b,c, ts)
    /// PLAN AFTER: sink(start, end, sum) < proj(start, end, sum), agg(sum(a) -> sum, c) < eventTime(ts) < proj(ts, a) source

    auto source = utils.createSource("aggregation", {"a", "b", "ts", "c"});
    auto eventTimeWA = EventTimeWatermarkAssignerLogicalOperator::create(
        source, FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("ts")].value()}, Windowing::TimeUnit{0});

    auto aggregation = WindowedAggregationLogicalOperator::create(
        eventTimeWA,
        std::vector<std::pair<TypedLogicalFunction<FieldAccessLogicalFunction>, std::optional<Identifier>>>{
            {FieldAccessLogicalFunction{eventTimeWA.getOutputSchema()[Identifier::parse("c")].value()}, Identifier::parse("c")}},
        std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation>{
            {.function = SumAggregationLogicalFunction{TypedLogicalFunction<FieldAccessLogicalFunction>{
                 FieldAccessLogicalFunction{eventTimeWA.getOutputSchema()[Identifier::parse("a")].value()}}},
             .name = Identifier::parse("sum")}},
        Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{0}}},
        Windowing::BoundEventTimeCharacteristic{
            .field = FieldAccessLogicalFunction{eventTimeWA.getOutputSchema()[Identifier::parse("ts")].value()}});


    auto projection = ProjectionLogicalOperator::create(
        aggregation,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("start"), FieldAccessLogicalFunction{aggregation.getOutputSchema()[Identifier::parse("start")].value()}},
            {Identifier::parse("end"), FieldAccessLogicalFunction{aggregation.getOutputSchema()[Identifier::parse("end")].value()}},
            {Identifier::parse("sum"), FieldAccessLogicalFunction{aggregation.getOutputSchema()[Identifier::parse("sum")].value()}},
        },
        ProjectionLogicalOperator::Asterisk{false});

    auto sink = utils.createSink(projection, "aggregation", {"start", "end", "sum"});
    auto plan = utils.createPlan(sink);

    auto pushed = ProjectionPushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());

    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(op1.getOutputSchema().size(), 3);
    ASSERT_TRUE(op1.getOutputSchema()[Identifier::parse("start")]);
    ASSERT_TRUE(op1.getOutputSchema()[Identifier::parse("end")]);
    ASSERT_TRUE(op1.getOutputSchema()[Identifier::parse("sum")]);

    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<WindowedAggregationLogicalOperator>());
    ASSERT_EQ(op2.getOutputSchema().size(), 4);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("start")]);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("end")]);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("sum")]);
    ASSERT_TRUE(op2.getOutputSchema()[Identifier::parse("c")]);

    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>());
    ASSERT_EQ(op3.getOutputSchema().size(), 3);
    ASSERT_TRUE(op3.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(op3.getOutputSchema()[Identifier::parse("c")]);
    ASSERT_TRUE(op3.getOutputSchema()[Identifier::parse("ts")]);

    auto op4 = op3.getChildren().at(0);
    ASSERT_TRUE(op4.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_EQ(op4.getOutputSchema().size(), 3);
    ASSERT_TRUE(op4.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(op4.getOutputSchema()[Identifier::parse("c")]);
    ASSERT_TRUE(op4.getOutputSchema()[Identifier::parse("ts")]);

    auto op5 = op4.getChildren().at(0);
    ASSERT_TRUE(op5.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_EQ(op5.getOutputSchema().size(), 4);
    ASSERT_TRUE(op5.getOutputSchema()[Identifier::parse("a")]);
    ASSERT_TRUE(op5.getOutputSchema()[Identifier::parse("b")]);
    ASSERT_TRUE(op5.getOutputSchema()[Identifier::parse("c")]);
    ASSERT_TRUE(op5.getOutputSchema()[Identifier::parse("ts")]);
}

TEST_F(ProjectionPushdownRuleTest, ValidateNoProjectionAddedIfFullSourceSchemaIsRequired)
{
    /// Plan BEFORE: sink < select < source
    /// Plan Afyer: unchaged.

    auto source = utils.createSource("noprojection", {"a", "b"});
    auto select = SelectionLogicalOperator::create(
        source,
        GreaterLogicalFunction{
            FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}});
    auto sink = utils.createSink(select, "sink", {"a", "b"});
    auto plan = utils.createPlan(sink);

    auto pushed = ProjectionPushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SourceDescriptorLogicalOperator>());
}

/// NOLINTEND(bugprone-unchecked-optional-access)

}
