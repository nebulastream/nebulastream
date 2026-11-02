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

#include <LoweringRules/LowerToPhysical/TraceSourcesOfInputOrigins.hpp>

#include <string>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Rules/Static/OriginIdInferenceRule.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class TraceSourcesOfInputOriginsTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("TraceSourcesOfInputOriginsTest.log", LogLevel::LOG_DEBUG); }

    static OriginId originOf(const LogicalOperator& op) { return (*op.getTraitSet().get<OutputOriginIdsTrait>())[0]; }

    static LogicalOperator join(
        const LogicalOperator& left,
        const LogicalOperator& right,
        const FieldAccessLogicalFunction& leftKey,
        const FieldAccessLogicalFunction& rightKey)
    {
        return JoinLogicalOperator::create(
            {left, right},
            EqualsLogicalFunction{leftKey, rightKey},
            Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{0}}},
            JoinLogicalOperator::JoinType::INNER_JOIN,
            JoinTimeCharacteristic{});
    }

    static FieldAccessLogicalFunction field(const LogicalOperator& op, const std::string& name)
    {
        return FieldAccessLogicalFunction{op.getOutputSchema().getFieldByName(Identifier::parse(name)).value()};
    }

    OptimizerTestUtils utils;
};

/// The outer join's left input origin is produced by the inner join, so it is driven by both sources below the inner join.
TEST_F(TraceSourcesOfInputOriginsTest, InputOriginOfAWindowMapsToTheSourcesBelowIt)
{
    const auto watermark1 = IngestionTimeWatermarkAssignerLogicalOperator::create(utils.createSource("sources1", {"a", "b"}));
    const auto watermark2 = IngestionTimeWatermarkAssignerLogicalOperator::create(utils.createSource("sources2", {"c", "d"}));
    const auto watermark3 = IngestionTimeWatermarkAssignerLogicalOperator::create(utils.createSource("sources3", {"e", "f"}));
    const auto innerJoin = join(watermark1, watermark2, field(watermark1, "a"), field(watermark2, "c"));
    /// Projects away the window start and end of the inner join, which would collide with the ones of the outer join.
    const auto innerKey = ProjectionLogicalOperator::create(
        innerJoin, {{Identifier::parse("x"), field(innerJoin, "a")}}, ProjectionLogicalOperator::Asterisk{false});
    const auto outerJoin = join(innerKey, watermark3, field(innerKey, "x"), field(watermark3, "e"));
    const auto plan
        = OriginIdInferenceRule{}.apply(utils.createPlan(utils.createSink(outerJoin, "sourcesSink", {"x", "e", "f", "start", "end"})));

    const auto optimizedOuterJoin = plan.getRootOperators().at(0).getChildren().at(0);
    const auto optimizedInnerJoin = optimizedOuterJoin.getChildren().at(0).getChildren().at(0);
    const auto source1 = optimizedInnerJoin.getChildren().at(0).getChildren().at(0);
    const auto source2 = optimizedInnerJoin.getChildren().at(1).getChildren().at(0);
    const auto source3 = optimizedOuterJoin.getChildren().at(1).getChildren().at(0);

    const auto sourcesOfInputOrigins = traceSourcesOfInputOrigins(optimizedOuterJoin.getChildren());

    EXPECT_EQ(sourcesOfInputOrigins.size(), 2);
    EXPECT_THAT(
        sourcesOfInputOrigins.at(originOf(optimizedInnerJoin)), ::testing::UnorderedElementsAre(originOf(source1), originOf(source2)));
    EXPECT_THAT(sourcesOfInputOrigins.at(originOf(source3)), ::testing::ElementsAre(originOf(source3)));
}

/// The inner join sits below the outer join, so the two are not siblings, even though they share sources.
TEST_F(TraceSourcesOfInputOriginsTest, NestedWindowsAreNotSiblings)
{
    const auto watermark1 = IngestionTimeWatermarkAssignerLogicalOperator::create(utils.createSource("sources1", {"a", "b"}));
    const auto watermark2 = IngestionTimeWatermarkAssignerLogicalOperator::create(utils.createSource("sources2", {"c", "d"}));
    const auto watermark3 = IngestionTimeWatermarkAssignerLogicalOperator::create(utils.createSource("sources3", {"e", "f"}));
    const auto innerJoin = join(watermark1, watermark2, field(watermark1, "a"), field(watermark2, "c"));
    const auto innerKey = ProjectionLogicalOperator::create(
        innerJoin, {{Identifier::parse("x"), field(innerJoin, "a")}}, ProjectionLogicalOperator::Asterisk{false});
    const auto outerJoin = join(innerKey, watermark3, field(innerKey, "x"), field(watermark3, "e"));
    const auto plan
        = OriginIdInferenceRule{}.apply(utils.createPlan(utils.createSink(outerJoin, "sourcesSink", {"x", "e", "f", "start", "end"})));

    EXPECT_THAT(findWindowsSharingSourcesWithSiblings(plan), ::testing::IsEmpty());
}

/// Both inner joins read the same two sources and neither is below the other, so each may throttle a source the other one waits for.
/// The outer join is above both and keeps throttling.
TEST_F(TraceSourcesOfInputOriginsTest, SiblingWindowsSharingASourceDoNotThrottle)
{
    const auto watermark1 = IngestionTimeWatermarkAssignerLogicalOperator::create(utils.createSource("sources1", {"a", "b"}));
    const auto watermark2 = IngestionTimeWatermarkAssignerLogicalOperator::create(utils.createSource("sources2", {"c", "d"}));
    const auto leftJoin = join(watermark1, watermark2, field(watermark1, "a"), field(watermark2, "c"));
    const auto rightJoin = join(watermark1, watermark2, field(watermark1, "b"), field(watermark2, "d"));
    const auto leftKey = ProjectionLogicalOperator::create(
        leftJoin, {{Identifier::parse("x"), field(leftJoin, "a")}}, ProjectionLogicalOperator::Asterisk{false});
    const auto rightKey = ProjectionLogicalOperator::create(
        rightJoin, {{Identifier::parse("y"), field(rightJoin, "b")}}, ProjectionLogicalOperator::Asterisk{false});
    const auto outerJoin = join(leftKey, rightKey, field(leftKey, "x"), field(rightKey, "y"));
    const auto plan
        = OriginIdInferenceRule{}.apply(utils.createPlan(utils.createSink(outerJoin, "sourcesSink", {"x", "y", "start", "end"})));

    const auto optimizedOuterJoin = plan.getRootOperators().at(0).getChildren().at(0);
    const auto optimizedLeftJoin = optimizedOuterJoin.getChildren().at(0).getChildren().at(0);
    const auto optimizedRightJoin = optimizedOuterJoin.getChildren().at(1).getChildren().at(0);

    const auto windowsWithoutThrottling = findWindowsSharingSourcesWithSiblings(plan);
    EXPECT_THAT(windowsWithoutThrottling, ::testing::UnorderedElementsAre(optimizedLeftJoin.getId(), optimizedRightJoin.getId()));

    for (const auto& [origin, sources] : traceThrottleableSourcesOfInputOrigins(optimizedLeftJoin, windowsWithoutThrottling))
    {
        EXPECT_THAT(sources, ::testing::IsEmpty()) << "input origin " << origin;
    }
    EXPECT_EQ(traceThrottleableSourcesOfInputOrigins(optimizedOuterJoin, windowsWithoutThrottling).size(), 2);
    for (const auto& [origin, sources] : traceThrottleableSourcesOfInputOrigins(optimizedOuterJoin, windowsWithoutThrottling))
    {
        EXPECT_THAT(sources, ::testing::Not(::testing::IsEmpty())) << "input origin " << origin;
    }
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
