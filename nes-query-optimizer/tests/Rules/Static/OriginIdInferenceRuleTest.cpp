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

#include <Rules/Static/OriginIdInferenceRule.hpp>

#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class OriginIdInferenceRuleTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("OriginIdInferenceRuleTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
};

TEST_F(OriginIdInferenceRuleTest, singleSinkTest)
{
    const auto source1 = utils.createSource("singleSink1", {"a", "b"});
    const auto source2 = utils.createSource("singleSink2", {"c", "d"});
    const auto watermark1 = IngestionTimeWatermarkAssignerLogicalOperator::create(source1);
    const auto watermark2 = IngestionTimeWatermarkAssignerLogicalOperator::create(source2);
    const auto join = JoinLogicalOperator::create(
        {watermark1, watermark2},
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{watermark1.getOutputSchema().getFieldByName(Identifier::parse("a")).value()},
            FieldAccessLogicalFunction{watermark2.getOutputSchema().getFieldByName(Identifier::parse("c")).value()}},
        Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{0}}},
        JoinLogicalOperator::JoinType::INNER_JOIN,
        JoinTimeCharacteristic{});

    const auto sink = utils.createSink(join, "singleSink", {"a", "b", "c", "d", "start", "end"});

    const auto plan = utils.createPlan(sink);

    auto optimized = OriginIdInferenceRule{}.apply(plan);

    auto op1 = optimized.getRootOperators().at(0);
    ASSERT_TRUE(op1.tryGetAs<SinkLogicalOperator>());
    ASSERT_TRUE(op1.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(op1.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*op1.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{4});


    auto op2 = op1.getChildren().at(0);
    ASSERT_TRUE(op2.tryGetAs<JoinLogicalOperator>());
    ASSERT_TRUE(op2.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(op2.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*op2.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{4});

    auto leftOp3 = op2.getChildren().at(0);
    ASSERT_TRUE(leftOp3.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>());
    ASSERT_TRUE(leftOp3.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(leftOp3.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*leftOp3.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{2});

    auto leftOp4 = leftOp3.getChildren().at(0);
    ASSERT_TRUE(leftOp4.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_TRUE(leftOp4.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(leftOp4.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*leftOp4.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{2});

    auto rightOp3 = op2.getChildren().at(1);
    ASSERT_TRUE(rightOp3.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>());
    ASSERT_TRUE(rightOp3.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(rightOp3.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*rightOp3.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{3});

    auto rightOp4 = rightOp3.getChildren().at(0);
    ASSERT_TRUE(rightOp4.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_TRUE(rightOp4.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(rightOp4.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*rightOp4.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{3});
}

TEST_F(OriginIdInferenceRuleTest, MultiSink)
{
    /// PLAN. The brackets indicate the expected OriginId
    ///
    ///                  sink1 [4] > select1 [4]             > watermark1 [2] > source1 [2]
    ///                                         \           /
    ///                                          > join1 [4]
    ///                                         /           \
    ///                         > projection [4]             > watermark2 [3] > source2 [3]
    ///                        /
    ///   sink2 [6] > join2 [6]
    ///                        \
    ///                         >  watermark3 [5] > source3 [5]


    const auto source1 = utils.createSource("multiSink1", {"a", "b"});
    const auto source2 = utils.createSource("multiSink2", {"c", "d"});
    const auto watermark1 = IngestionTimeWatermarkAssignerLogicalOperator::create(source1);
    const auto watermark2 = IngestionTimeWatermarkAssignerLogicalOperator::create(source2);
    const auto join1 = JoinLogicalOperator::create(
        {watermark1, watermark2},
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{watermark1.getOutputSchema().getFieldByName(Identifier::parse("a")).value()},
            FieldAccessLogicalFunction{watermark2.getOutputSchema().getFieldByName(Identifier::parse("c")).value()}},
        Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{0}}},
        JoinLogicalOperator::JoinType::INNER_JOIN,
        JoinTimeCharacteristic{});

    const auto select1 = SelectionLogicalOperator::create(
        join1,
        GreaterLogicalFunction{
            FieldAccessLogicalFunction{join1.getOutputSchema().getFieldByName(Identifier::parse("a")).value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE}, "0"}});

    const auto sink1 = utils.createSink(select1, "multiSink1", {"a", "b", "c", "d", "start", "end"});
    const std::vector<std::pair<Identifier, LogicalFunction>> projections{
        {Identifier::parse("a"), FieldAccessLogicalFunction{join1.getOutputSchema()[Identifier::parse("a")].value()}},
        {Identifier::parse("b"), FieldAccessLogicalFunction{join1.getOutputSchema()[Identifier::parse("b")].value()}}};

    auto projection = ProjectionLogicalOperator::create(join1, projections, ProjectionLogicalOperator::Asterisk{false});

    const auto source3 = utils.createSource("multiSink3", {"e", "f"});
    const auto watermark3 = IngestionTimeWatermarkAssignerLogicalOperator::create(source3);


    const auto join2 = JoinLogicalOperator::create(
        {projection, watermark3},
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{projection.getOutputSchema().getFieldByName(Identifier::parse("a")).value()},
            FieldAccessLogicalFunction{watermark3.getOutputSchema().getFieldByName(Identifier::parse("e")).value()}},
        Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{0}}},
        JoinLogicalOperator::JoinType::INNER_JOIN,
        JoinTimeCharacteristic{});

    const auto sink2 = utils.createSink(join2, "multiSink2", {"a", "b", "e", "f", "start", "end"});


    const auto plan = utils.createPlan({sink1, sink2});

    auto optimized = OriginIdInferenceRule{}.apply(plan);

    auto optSink1 = optimized.getRootOperators().at(0);
    ASSERT_TRUE(optSink1.tryGetAs<SinkLogicalOperator>());
    ASSERT_TRUE(optSink1.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(optSink1.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*optSink1.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{4});

    auto optSelect1 = optSink1.getChildren().at(0);
    ASSERT_TRUE(optSelect1.tryGetAs<SelectionLogicalOperator>());
    ASSERT_TRUE(optSelect1.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(optSelect1.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*optSelect1.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{4});

    auto optJoin1 = optSelect1.getChildren().at(0);
    ASSERT_TRUE(optJoin1.tryGetAs<JoinLogicalOperator>());
    ASSERT_TRUE(optJoin1.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(optJoin1.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*optJoin1.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{4});

    auto optWatermark1 = optJoin1.getChildren().at(0);
    ASSERT_TRUE(optWatermark1.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>());
    ASSERT_TRUE(optWatermark1.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(optWatermark1.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*optWatermark1.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{2});

    auto optSource1 = optWatermark1.getChildren().at(0);
    ASSERT_TRUE(optSource1.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_TRUE(optSource1.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(optSource1.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*optSource1.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{2});

    auto optWatermark2 = optJoin1.getChildren().at(1);
    ASSERT_TRUE(optWatermark2.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>());
    ASSERT_TRUE(optWatermark2.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(optWatermark2.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*optWatermark2.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{3});

    auto optSource2 = optWatermark2.getChildren().at(0);
    ASSERT_TRUE(optSource2.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_TRUE(optSource2.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(optSource2.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*optSource2.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{3});

    auto optSink2 = optimized.getRootOperators().at(1);
    ASSERT_TRUE(optSink2.tryGetAs<SinkLogicalOperator>());
    ASSERT_TRUE(optSink2.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(optSink2.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*optSink2.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{6});

    auto optJoin2 = optSink2.getChildren().at(0);
    ASSERT_TRUE(optJoin2.tryGetAs<JoinLogicalOperator>());
    ASSERT_TRUE(optJoin2.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(optJoin2.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*optJoin2.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{6});

    auto optProjection = optJoin2.getChildren().at(0);
    ASSERT_TRUE(optProjection.tryGetAs<ProjectionLogicalOperator>());
    ASSERT_TRUE(optProjection.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(optProjection.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*optProjection.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{4});

    auto optJoin1Alt = optProjection.getChildren().at(0);
    ASSERT_EQ(optJoin1Alt, optJoin1);

    auto optWatermark3 = optJoin2.getChildren().at(1);
    ASSERT_TRUE(optWatermark3.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>());
    ASSERT_TRUE(optWatermark3.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(optWatermark3.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*optWatermark3.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{5});

    auto optSource3 = optWatermark3.getChildren().at(0);
    ASSERT_TRUE(optSource3.tryGetAs<SourceDescriptorLogicalOperator>());
    ASSERT_TRUE(optSource3.getTraitSet().contains<OutputOriginIdsTrait>());
    ASSERT_EQ(optSource3.getTraitSet().get<OutputOriginIdsTrait>()->size(), 1);
    ASSERT_EQ((*optSource3.getTraitSet().get<OutputOriginIdsTrait>())[0], OriginId{5});
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
