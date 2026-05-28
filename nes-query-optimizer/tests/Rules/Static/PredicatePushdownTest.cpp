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
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
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
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Static/PredicatePushdownRule.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace
{
class PredicatePushdownTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("PredicatePushdownTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
};
}

TEST_F(PredicatePushdownTest, ValidatePushBeyondSelection)
{
    auto source = utils.createSource("selection", {"source_value", "source_ts", "selection"});

    auto predicate1 = EqualsLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("source_value")].value()},
        ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}};
    auto predicate2 = GreaterLogicalFunction{
        FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("source_ts")].value()},
        ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}};
    auto selection1 = SelectionLogicalOperator::create(source, predicate1);
    auto selection2 = SelectionLogicalOperator::create(selection1, predicate2);
    auto sink = utils.createSink(selection2, "selection", {"source_value", "source_ts", "selection"});

    auto plan = utils.createPlan(sink);

    auto pushed = PredicatePushdownRule{}.apply(plan);

    ASSERT_EQ(pushed.getRootOperators().size(), 1);
    auto root = pushed.getRootOperators()[0];
    ASSERT_TRUE(root.tryGetAs<SinkLogicalOperator>());
    auto op1 = root.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>().value()->getPredicate().tryGetAs<AndLogicalFunction>());
    ASSERT_EQ(op1.getChildren().size(), 1);
    ASSERT_TRUE(op1.getChildren().at(0).tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(PredicatePushdownTest, ValidatePushBeyondProjection)
{
    auto source = utils.createSource("projection", {"source_value", "source_id", "source_ts"});

    const std::vector<std::pair<Identifier, LogicalFunction>> projections{
        {Identifier::parse("source_value"),
         FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("source_value")].value()}},
        {Identifier::parse("new"), ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}},
    };
    auto projection = ProjectionLogicalOperator::create(source, projections, ProjectionLogicalOperator::Asterisk(false));

    auto predicate = AndLogicalFunction{
        GreaterLogicalFunction{
            FieldAccessLogicalFunction{projection.getOutputSchema()[Identifier::parse("source_value")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "42"}},
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{projection.getOutputSchema()[Identifier::parse("new")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "1337"}}};

    auto selection = SelectionLogicalOperator::create(projection, predicate);


    auto sink = utils.createSink(selection, "projection", {"source_value", "new"});
    auto plan = utils.createPlan(sink);


    auto pushed = PredicatePushdownRule{}.apply(plan);


    ASSERT_EQ(pushed.getRootOperators().size(), 1);
    auto root = pushed.getRootOperators()[0];
    ASSERT_TRUE(root.tryGetAs<SinkLogicalOperator>());
    auto op1 = root.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>().value()->getPredicate().tryGetAs<EqualsLogicalFunction>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<ProjectionLogicalOperator>());
    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op3.tryGetAs<SelectionLogicalOperator>().value()->getPredicate().tryGetAs<GreaterLogicalFunction>());
    auto op4 = op3.getChildren().at(0);
    ASSERT_TRUE(op4.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(PredicatePushdownTest, ValidatePushBeyondProjectionWithAsterisk)
{
    /// BEFORE: Source(source_value, source_id, source_ts) < Projection(*, 0 AS new) < Selection(source_value > 42 AND new = 1337)) < Sink(source_value, new, source_id, source_ts)
    /// AFTER:  Source(source_value, source_id, source_ts) < Selection (source_value > 42) < Projection(*, 0 AS new) < Selection(new = 1337) < Sink(source_value, new, source_id, source_ts)

    auto source = utils.createSource("projectionWithAsterisk", {"source_value", "source_id", "source_ts"});

    const std::vector<std::pair<Identifier, LogicalFunction>> projections{
        {Identifier::parse("new"), ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}},
    };
    auto projection = ProjectionLogicalOperator::create(source, projections, ProjectionLogicalOperator::Asterisk(true));

    auto predicate = AndLogicalFunction{
        GreaterLogicalFunction{
            FieldAccessLogicalFunction{projection.getOutputSchema()[Identifier::parse("source_value")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "42"}},
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{projection.getOutputSchema()[Identifier::parse("new")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "1337"}}};

    auto selection = SelectionLogicalOperator::create(projection, predicate);


    auto sink = utils.createSink(selection, "projection", {"source_value", "new", "source_id", "source_ts"});
    auto plan = utils.createPlan(sink);


    auto pushed = PredicatePushdownRule{}.apply(plan);


    ASSERT_EQ(pushed.getRootOperators().size(), 1);
    auto root = pushed.getRootOperators()[0];
    ASSERT_TRUE(root.tryGetAs<SinkLogicalOperator>());
    auto op1 = root.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>().value()->getPredicate().tryGetAs<EqualsLogicalFunction>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<ProjectionLogicalOperator>());
    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op3.tryGetAs<SelectionLogicalOperator>().value()->getPredicate().tryGetAs<GreaterLogicalFunction>());
    auto op4 = op3.getChildren().at(0);
    ASSERT_TRUE(op4.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(PredicatePushdownTest, ValidatePushBeyondJoin)
{
    /// sink < selection < join < (source), (source)
    /// sink < selection < join < (selection < source), (selection, source)


    auto sourceLeft = utils.createSource("join_left", {"left_value", "left_id", "left_ts"});
    auto sourceRight = utils.createSource("join_right", {"right_value", "right_id", "right_ts"});


    auto joinFunction = EqualsLogicalFunction{
        FieldAccessLogicalFunction{sourceLeft.getOutputSchema()[Identifier::parse("left_value")].value()},
        FieldAccessLogicalFunction{sourceRight.getOutputSchema()[Identifier::parse("right_value")].value()}};

    auto join = JoinLogicalOperator::create(
        std::array<LogicalOperator, 2>{sourceLeft, sourceRight},
        joinFunction,
        Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{0}}},
        JoinLogicalOperator::JoinType{},
        JoinTimeCharacteristic{});


    auto predicate = AndLogicalFunction{
        AndLogicalFunction{
            GreaterEqualsLogicalFunction{
                FieldAccessLogicalFunction{join.getOutputSchema()[Identifier::parse("left_value")].value()},
                ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}},
            LessLogicalFunction{
                FieldAccessLogicalFunction{join.getOutputSchema()[Identifier::parse("right_value")].value()},
                ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}}},
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{join.getOutputSchema()[Identifier::parse("left_value")].value()},
            FieldAccessLogicalFunction{join.getOutputSchema()[Identifier::parse("right_value")].value()}}};


    auto selection = SelectionLogicalOperator::create(join, predicate);

    auto sink
        = utils.createSink(selection, "join", {"left_id", "left_value", "left_ts", "right_id", "right_value", "right_ts", "start", "end"});
    auto plan = utils.createPlan(sink);

    auto pushed = PredicatePushdownRule{}.apply(plan);


    auto root = pushed.getRootOperators().at(0);
    ASSERT_TRUE(root.tryGetAs<SinkLogicalOperator>());
    auto op1 = root.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>().value()->getPredicate().tryGetAs<EqualsLogicalFunction>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<JoinLogicalOperator>());
    auto leftOp1 = op2.getChildren().at(0);
    ASSERT_TRUE(leftOp1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(leftOp1.tryGetAs<SelectionLogicalOperator>().value()->getPredicate().tryGetAs<GreaterEqualsLogicalFunction>());
    auto leftOp2 = leftOp1.getChildren().at(0);
    ASSERT_TRUE(leftOp2.tryGetAs<SourceDescriptorLogicalOperator>());
    auto rightOp1 = op2.getChildren().at(1);
    ASSERT_TRUE(rightOp1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(rightOp1.tryGetAs<SelectionLogicalOperator>().value()->getPredicate().tryGetAs<LessLogicalFunction>());
    auto rightOp2 = rightOp1.getChildren().at(0);
    ASSERT_TRUE(rightOp2.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(PredicatePushdownTest, ValidatePushBeyondUnion)
{
    /// BEFORE: sink < selection < union < (source, source)
    /// AFTER: sink < union < (selection < source, selection < source)

    auto sourceLeft = utils.createSource("union_left", {"union_value", "union_id", "union_ts"});
    auto sourceRight = utils.createSource("union_right", {"union_value", "union_id", "union_ts"});

    auto unionOp = UnionLogicalOperator::create(std::vector<LogicalOperator>{sourceLeft, sourceRight});

    auto predicate = EqualsLogicalFunction{
        FieldAccessLogicalFunction{unionOp.getOutputSchema()[Identifier::parse("union_value")].value()},
        ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "0"}};

    auto selection = SelectionLogicalOperator::create(unionOp, predicate);

    auto sink = utils.createSink(selection, "union", {"union_id", "union_value", "union_ts"});
    auto plan = utils.createPlan(sink);

    auto pushed = PredicatePushdownRule{}.apply(plan);

    auto root = pushed.getRootOperators().at(0);
    ASSERT_TRUE(root.tryGetAs<SinkLogicalOperator>());
    auto op1 = root.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<UnionLogicalOperator>());
    auto leftOp1 = op1.getChildren().at(0);
    ASSERT_TRUE(leftOp1.tryGetAs<SelectionLogicalOperator>());
    auto leftOp2 = leftOp1.getChildren().at(0);
    ASSERT_TRUE(leftOp2.tryGetAs<SourceDescriptorLogicalOperator>());
    auto rightOp1 = op1.getChildren().at(1);
    ASSERT_TRUE(rightOp1.tryGetAs<SelectionLogicalOperator>());
    auto rightOp2 = rightOp1.getChildren().at(0);
    ASSERT_TRUE(rightOp2.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(PredicatePushdownTest, ValidatePushBeyondWaterMarkAssigner)
{
    /// BEFORE: Sink < Selection < IngestionWatermarkAssinger < EventTimeWatermarkAssigner < Source
    /// AFTER: Sink < IngestionWatermarkAssinger < EventTimeWatermarkAssigner < Selection < Source


    auto source = utils.createSource("watermark_assigner", {"watermark_assigner_id", "watermark_assigner_value", "watermark_assigner_ts"});
    auto ingestionTimeWA = IngestionTimeWatermarkAssignerLogicalOperator::create(source);
    auto eventTimeWA = EventTimeWatermarkAssignerLogicalOperator::create(
        ingestionTimeWA,
        FieldAccessLogicalFunction{ingestionTimeWA.getOutputSchema()[Identifier::parse("watermark_assigner_value")].value()},
        Windowing::TimeUnit{0});
    auto predicate = EqualsLogicalFunction{
        FieldAccessLogicalFunction{eventTimeWA.getOutputSchema()[Identifier::parse("watermark_assigner_value")].value()},
        ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "0"}};
    auto selection = SelectionLogicalOperator::create(eventTimeWA, predicate);
    auto sink
        = utils.createSink(selection, "watermark_assigner", {"watermark_assigner_id", "watermark_assigner_value", "watermark_assigner_ts"});
    auto plan = utils.createPlan(sink);

    auto pushed = PredicatePushdownRule{}.apply(plan);

    auto root = pushed.getRootOperators().at(0);
    ASSERT_TRUE(root.tryGetAs<SinkLogicalOperator>());
    auto op1 = root.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>());
    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<SelectionLogicalOperator>());
    auto op4 = op3.getChildren().at(0);
    ASSERT_TRUE(op4.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(PredicatePushdownTest, ValidatePushBeyondWindowedAggregation)
{
    /// BEFORE: Sink < Selection < WindowedAgg < EventTimeWatermarkAssigner < Source
    /// AFTER: Sink < Selection < WindowedAgg < EventTimeWatermarkAssigner < Selection < Source

    auto source = utils.createSource("aggregation", {"aggregation_id", "aggregation_value", "aggregation_ts"});

    auto eventTime = EventTimeWatermarkAssignerLogicalOperator::create(
        source, FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("aggregation_ts")].value()}, Windowing::TimeUnit{0});

    auto aggregation = WindowedAggregationLogicalOperator::create(
        eventTime,
        std::vector<std::pair<TypedLogicalFunction<FieldAccessLogicalFunction>, std::optional<Identifier>>>{
            {TypedLogicalFunction<FieldAccessLogicalFunction>{
                 FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("aggregation_value")].value()}},
             Identifier::parse("aggregation_value")}},
        std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation>{
            {.function = CountAggregationLogicalFunction{TypedLogicalFunction<FieldAccessLogicalFunction>{
                                                             FieldAccessLogicalFunction{
                                                                 source.getOutputSchema()[Identifier::parse("aggregation_id")].value()}},
                                                         false},
             .name = Identifier::parse("count")}},
        Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{0}}},
        Windowing::BoundEventTimeCharacteristic{
            .field = FieldAccessLogicalFunction{eventTime.getOutputSchema()[Identifier::parse("aggregation_ts")].value()}});

    auto predicate = AndLogicalFunction{
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{aggregation.getOutputSchema()[Identifier::parse("aggregation_value")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "0"}},
        GreaterLogicalFunction{
            FieldAccessLogicalFunction{aggregation.getOutputSchema()[Identifier::parse("count")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "0"}}};

    auto selection = SelectionLogicalOperator::create(aggregation, predicate);

    auto sink = utils.createSink(selection, "aggregation", {"aggregation_value", "count", "START", "END"});
    auto plan = utils.createPlan(sink);

    auto pushed = PredicatePushdownRule{}.apply(plan);


    auto root = pushed.getRootOperators().at(0);
    ASSERT_TRUE(root.tryGetAs<SinkLogicalOperator>());
    auto op1 = root.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op1.tryGetAs<SelectionLogicalOperator>().value()->getPredicate().tryGetAs<GreaterLogicalFunction>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<WindowedAggregationLogicalOperator>());
    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>());
    auto op4 = op3.getChildren().at(0);
    ASSERT_TRUE(op4.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(op4.tryGetAs<SelectionLogicalOperator>().value()->getPredicate().tryGetAs<EqualsLogicalFunction>());
    auto op5 = op4.getChildren().at(0);
    ASSERT_TRUE(op5.tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(PredicatePushdownTest, PushBeyondhasAsterisk)
{
    /// BEFORE: SINK < SELECT < PROJECT < SOURCE
    /// AFTER: SINK < PROJECT < SELECT < SOURCE
    auto source = utils.createSource("beyondhasAsterisk", {"a", "b"});
    auto projection = ProjectionLogicalOperator::create(source, {}, ProjectionLogicalOperator::Asterisk{true});
    auto selection = SelectionLogicalOperator::create(
        projection,
        GreaterEqualsLogicalFunction{
            FieldAccessLogicalFunction{projection.getOutputSchema()[Identifier::parse("A")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "0"}});
    auto sink = utils.createSink(selection, "beyondhasAsterisk", {"a", "b"});

    auto plan = utils.createPlan(sink);
    auto pushed = PredicatePushdownRule{}.apply(plan);

    auto op0 = pushed.getRootOperators().at(0);
    ASSERT_TRUE(op0.tryGetAs<SinkLogicalOperator>());
    auto op1 = op0.getChildren().at(0);
    ASSERT_TRUE(op1.tryGetAs<ProjectionLogicalOperator>());
    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<SelectionLogicalOperator>());
    auto op3 = op2.getChildren().at(0);
    ASSERT_TRUE(op3.tryGetAs<SourceDescriptorLogicalOperator>());
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
