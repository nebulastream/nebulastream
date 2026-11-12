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

#include <cstddef>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Rules/Static/OriginIdInferenceRule.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>

namespace NES
{
namespace
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class TraceSourcesOfInputOriginsTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("TraceSourcesOfInputOriginsTest.log", LogLevel::LOG_DEBUG); }

    static OriginId originOf(const LogicalOperator& op) { return *op.getTraitSet().get<OutputOriginIdsTrait>()->begin(); }

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

    static LogicalOperator withOrigins(const LogicalOperator& op, std::vector<OriginId> origins)
    {
        return op.withTraitSet(TraitSet{OutputOriginIdsTrait{std::move(origins)}});
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

/// The upstream join keeps its throttle; the downstream join must not also control the same sources.
TEST_F(TraceSourcesOfInputOriginsTest, NestedWindowsKeepOnlyTheUpstreamThrottle)
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

    const auto optimizedOuterJoin = plan.getRootOperators().at(0).getChildren().at(0);
    const auto optimizedInnerJoin = optimizedOuterJoin.getChildren().at(0).getChildren().at(0);
    const auto analysis = analyzeWatermarkGaps(plan);

    EXPECT_THAT(analysis->windowsWithoutThrottling, ::testing::ElementsAre(optimizedOuterJoin));
    for (const auto& [origin, sources] : traceThrottleableSourcesOfInputOrigins(optimizedInnerJoin, analysis))
    {
        EXPECT_FALSE(sources.empty()) << "input origin " << origin;
    }
    for (const auto& [origin, sources] : traceThrottleableSourcesOfInputOrigins(optimizedOuterJoin, analysis))
    {
        EXPECT_TRUE(sources.empty()) << "input origin " << origin;
    }
}

/// Both inner joins conflict with each other. The conservative policy also disables their downstream join, even when the
/// upstream controllers themselves are disabled, rather than trying to reassign ownership after resolving sibling conflicts.
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

    const auto analysis = analyzeWatermarkGaps(plan);
    const auto& windowsWithoutThrottling = analysis->windowsWithoutThrottling;
    EXPECT_THAT(windowsWithoutThrottling, ::testing::UnorderedElementsAre(optimizedLeftJoin, optimizedRightJoin, optimizedOuterJoin));
    EXPECT_TRUE(analysis->sourcesByWindow.contains(optimizedLeftJoin));
    EXPECT_TRUE(analysis->sourcesByWindow.contains(optimizedRightJoin));

    for (const auto& [origin, sources] : traceThrottleableSourcesOfInputOrigins(optimizedLeftJoin, analysis))
    {
        EXPECT_THAT(sources, ::testing::IsEmpty()) << "input origin " << origin;
    }
    EXPECT_EQ(traceThrottleableSourcesOfInputOrigins(optimizedOuterJoin, analysis).size(), 2);
    for (const auto& [origin, sources] : traceThrottleableSourcesOfInputOrigins(optimizedOuterJoin, analysis))
    {
        EXPECT_THAT(sources, ::testing::IsEmpty()) << "input origin " << origin;
    }
    /// Clearing a lowering rule's copy must not mutate shared provenance.
    for (const auto& [origin, sources] : analysis->sourcesByWindow.at(optimizedLeftJoin))
    {
        EXPECT_FALSE(sources.empty()) << "input origin " << origin;
    }
    EXPECT_EQ(findWindowsWithConflictingThrottles(plan), windowsWithoutThrottling);
    EXPECT_EQ(
        traceThrottleableSourcesOfInputOrigins(optimizedLeftJoin, analysis),
        traceThrottleableSourcesOfInputOrigins(optimizedLeftJoin, windowsWithoutThrottling));
}

/// W1(A, B) and W2(reassigned W1 output, A) must not both apply pressure, even if W1's output is shifted far ahead in event time.
TEST_F(TraceSourcesOfInputOriginsTest, DownstreamThrottleStaysDisabledAcrossWatermarkReassignment)
{
    const auto sourceA = withOrigins(utils.createSource("reassignedA", {"a"}), {OriginId(1)});
    const auto sourceB = withOrigins(utils.createSource("reassignedB", {"b"}), {OriginId(2)});
    const auto upstream = withOrigins(join(sourceA, sourceB, field(sourceA, "a"), field(sourceB, "b")), {OriginId(3)});
    const auto shifted = withOrigins(
        ProjectionLogicalOperator::create(
            upstream,
            {{Identifier::parse("shiftedTs"),
              AddLogicalFunction{
                  field(upstream, "start"),
                  ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::UINT64), "1000"}}}},
            ProjectionLogicalOperator::Asterisk{false}),
        {OriginId(3)});
    const auto reassigned = withOrigins(
        EventTimeWatermarkAssignerLogicalOperator::create(shifted, field(shifted, "shiftedTs"), Windowing::TimeUnit::Milliseconds()),
        {OriginId(3)});
    const auto downstream = withOrigins(join(reassigned, sourceA, field(reassigned, "shiftedTs"), field(sourceA, "a")), {OriginId(4)});
    const auto analysis = analyzeWatermarkGaps(utils.createPlan(downstream));

    EXPECT_THAT(analysis->windowsWithoutThrottling, ::testing::ElementsAre(downstream));
    const auto& upstreamSources = analysis->sourcesByWindow.at(upstream);
    EXPECT_THAT(upstreamSources.at(OriginId(1)), ::testing::ElementsAre(OriginId(1)));
    EXPECT_THAT(upstreamSources.at(OriginId(2)), ::testing::ElementsAre(OriginId(2)));
    const auto& downstreamSources = analysis->sourcesByWindow.at(downstream);
    EXPECT_THAT(downstreamSources.at(OriginId(3)), ::testing::UnorderedElementsAre(OriginId(1), OriginId(2)));
    EXPECT_THAT(downstreamSources.at(OriginId(1)), ::testing::ElementsAre(OriginId(1)));
    for (const auto& [origin, sources] : traceThrottleableSourcesOfInputOrigins(downstream, analysis))
    {
        EXPECT_TRUE(sources.empty()) << "input origin " << origin;
    }
}

TEST_F(TraceSourcesOfInputOriginsTest, PlansWithoutWindowsDoNotNeedOriginProvenance)
{
    const auto plan = utils.createPlan(utils.createSource("stateless", {"a"}));
    const auto analysis = analyzeWatermarkGaps(plan);

    EXPECT_TRUE(analysis->sourcesByWindow.empty());
    EXPECT_TRUE(analysis->windowsWithoutThrottling.empty());
}

/// A shared source receives both false and true downward contexts; provenance is needed if any parent feeds a window.
TEST_F(TraceSourcesOfInputOriginsTest, SharedSourceNeededByOneRootGetsProvenance)
{
    const auto left = withOrigins(utils.createSource("sharedLeft", {"a"}), {OriginId(1)});
    const auto right = withOrigins(utils.createSource("sharedRight", {"b"}), {OriginId(2)});
    const auto window = withOrigins(join(left, right, field(left, "a"), field(right, "b")), {OriginId(3)});
    /// This unrelated source deliberately has no origin trait: only subgraphs below windows need provenance.
    const auto unrelated = utils.createSource("unrelated", {"c"});
    const auto plan = utils.createPlan(
        std::vector<LogicalOperator>{
            utils.createSink(left, "passthroughSink", {"a"}),
            utils.createSink(window, "windowSink", {"a", "b", "start", "end"}),
            utils.createSink(unrelated, "unrelatedSink", {"c"})});
    const auto analysis = analyzeWatermarkGaps(plan);

    ASSERT_EQ(analysis->sourcesByWindow.size(), 1);
    EXPECT_TRUE(analysis->windowsWithoutThrottling.empty());
    const auto& sources = analysis->sourcesByWindow.at(window);
    EXPECT_THAT(sources.at(OriginId(1)), ::testing::ElementsAre(OriginId(1)));
    EXPECT_THAT(sources.at(OriginId(2)), ::testing::ElementsAre(OriginId(2)));
}

/// The standalone helper accepts an input forest whose roots can also be descendants of one another.
TEST_F(TraceSourcesOfInputOriginsTest, StandaloneTracingSupportsAnInputAndItsDescendant)
{
    const auto left = withOrigins(utils.createSource("overlapLeft", {"a"}), {OriginId(1)});
    const auto right = withOrigins(utils.createSource("overlapRight", {"b"}), {OriginId(2)});
    const auto window = withOrigins(join(left, right, field(left, "a"), field(right, "b")), {OriginId(3)});
    const auto sources = traceSourcesOfInputOrigins({window, left});

    ASSERT_EQ(sources.size(), 2);
    EXPECT_THAT(sources.at(OriginId(3)), ::testing::UnorderedElementsAre(OriginId(1), OriginId(2)));
    EXPECT_THAT(sources.at(OriginId(1)), ::testing::ElementsAre(OriginId(1)));
}

/// Disjoint sibling windows have no common source and must remain throttleable.
TEST_F(TraceSourcesOfInputOriginsTest, DisjointSiblingWindowsKeepThrottling)
{
    const auto source1 = IngestionTimeWatermarkAssignerLogicalOperator::create(utils.createSource("disjoint1", {"a", "b"}));
    const auto source2 = IngestionTimeWatermarkAssignerLogicalOperator::create(utils.createSource("disjoint2", {"c", "d"}));
    const auto source3 = IngestionTimeWatermarkAssignerLogicalOperator::create(utils.createSource("disjoint3", {"e", "f"}));
    const auto source4 = IngestionTimeWatermarkAssignerLogicalOperator::create(utils.createSource("disjoint4", {"g", "h"}));
    const auto left = join(source1, source2, field(source1, "a"), field(source2, "c"));
    const auto right = join(source3, source4, field(source3, "e"), field(source4, "g"));
    const auto leftKey
        = ProjectionLogicalOperator::create(left, {{Identifier::parse("x"), field(left, "a")}}, ProjectionLogicalOperator::Asterisk{false});
    const auto rightKey = ProjectionLogicalOperator::create(
        right, {{Identifier::parse("y"), field(right, "e")}}, ProjectionLogicalOperator::Asterisk{false});
    const auto outer = join(leftKey, rightKey, field(leftKey, "x"), field(rightKey, "y"));
    const auto plan = OriginIdInferenceRule{}.apply(utils.createPlan(utils.createSink(outer, "disjointSink", {"x", "y", "start", "end"})));

    const auto optimizedOuter = plan.getRootOperators().at(0).getChildren().at(0);
    const auto analysis = analyzeWatermarkGaps(plan);
    EXPECT_THAT(analysis->windowsWithoutThrottling, ::testing::ElementsAre(optimizedOuter));
    EXPECT_EQ(analysis->sourcesByWindow.size(), 3);
}

/// A long ancestry chain leaves only its upstream-most multi-origin window eligible to control the shared sources.
TEST_F(TraceSourcesOfInputOriginsTest, DeeplyNestedWindowsKeepOnlyTheUpstreamThrottle)
{
    constexpr size_t windowCount = 64;
    LogicalOperator input = IngestionTimeWatermarkAssignerLogicalOperator::create(utils.createSource("deep0", {"key0"}));
    for (size_t index = 1; index <= windowCount; ++index)
    {
        const auto nextKey = "key" + std::to_string(index);
        const auto next
            = IngestionTimeWatermarkAssignerLogicalOperator::create(utils.createSource("deep" + std::to_string(index), {nextKey}));
        const auto current = join(input, next, field(input, "key0"), field(next, nextKey));
        input = ProjectionLogicalOperator::create(
            current, {{Identifier::parse("key0"), field(current, "key0")}}, ProjectionLogicalOperator::Asterisk{false});
    }
    const auto plan = OriginIdInferenceRule{}.apply(utils.createPlan(utils.createSink(input, "deepSink", {"key0"})));
    const auto analysis = analyzeWatermarkGaps(plan);

    EXPECT_EQ(analysis->sourcesByWindow.size(), windowCount);
    EXPECT_EQ(analysis->windowsWithoutThrottling.size(), windowCount - 1);
    auto current = plan.getRootOperators().at(0).getChildren().at(0).getChildren().at(0);
    for (size_t index = 1; index < windowCount; ++index)
    {
        EXPECT_TRUE(analysis->windowsWithoutThrottling.contains(current));
        current = current.getChildren().at(0).getChildren().at(0);
    }
    EXPECT_FALSE(analysis->windowsWithoutThrottling.contains(current));
}

/// Network sources carry several upstream origins, but their backpressure channel belongs to their first output origin.
TEST_F(TraceSourcesOfInputOriginsTest, MultiOriginSourceUsesItsFirstOriginForEveryCarriedOrigin)
{
    const auto network = withOrigins(utils.createSource("networkLike", {"a"}), {OriginId(1), OriginId(2)});
    const auto other = withOrigins(utils.createSource("other", {"b"}), {OriginId(3)});
    const auto window = withOrigins(join(network, other, field(network, "a"), field(other, "b")), {OriginId(4)});
    const auto analysis = analyzeWatermarkGaps(utils.createPlan(window));
    const auto& sources = analysis->sourcesByWindow.at(window);

    ASSERT_EQ(sources.size(), 3);
    EXPECT_THAT(sources.at(OriginId(1)), ::testing::ElementsAre(OriginId(1)));
    EXPECT_THAT(sources.at(OriginId(2)), ::testing::ElementsAre(OriginId(1)));
    EXPECT_THAT(sources.at(OriginId(3)), ::testing::ElementsAre(OriginId(3)));
}

/// There are exponentially many paths through these repeated diamonds, but only linearly many distinct operators.
TEST_F(TraceSourcesOfInputOriginsTest, RepeatedDiamondPathsDoNotDuplicateSourcesOrWindows)
{
    const auto left = withOrigins(utils.createSource("diamondLeft", {"a"}), {OriginId(1)});
    const auto right = withOrigins(utils.createSource("diamondRight", {"b"}), {OriginId(2)});
    const auto inner = withOrigins(join(left, right, field(left, "a"), field(right, "b")), {OriginId(3)});
    auto input = withOrigins(
        ProjectionLogicalOperator::create(inner, {{Identifier::parse("x"), field(inner, "a")}}, ProjectionLogicalOperator::Asterisk{false}),
        {OriginId(3)});
    constexpr size_t diamondCount = 32;
    for (size_t index = 0; index < diamondCount; ++index)
    {
        input = withOrigins(UnionLogicalOperator::create({input, input}), {OriginId(3)});
    }
    const auto other = withOrigins(utils.createSource("diamondOther", {"c"}), {OriginId(4)});
    const auto outer = withOrigins(join(input, other, field(input, "x"), field(other, "c")), {OriginId(5)});
    const auto analysis = analyzeWatermarkGaps(utils.createPlan(outer));

    ASSERT_EQ(analysis->sourcesByWindow.size(), 2);
    EXPECT_THAT(analysis->windowsWithoutThrottling, ::testing::ElementsAre(outer));
    EXPECT_THAT(analysis->sourcesByWindow.at(outer).at(OriginId(3)), ::testing::UnorderedElementsAre(OriginId(1), OriginId(2)));
    EXPECT_THAT(analysis->sourcesByWindow.at(outer).at(OriginId(4)), ::testing::ElementsAre(OriginId(4)));
}

/// Source instances must stay distinct even if their operator ids collide within the same plan.
TEST_F(TraceSourcesOfInputOriginsTest, SourceProvenanceUsesIdentityRatherThanOperatorIds)
{
    const auto left = withOrigins(utils.createSource("identityLeft", {"a"}), {OriginId(1)});
    const auto originalRight = withOrigins(utils.createSource("identityRight", {"b"}), {OriginId(2)});
    const auto right = originalRight->withOperatorId(left.getId());
    const auto window = withOrigins(join(left, right, field(left, "a"), field(right, "b")), {OriginId(3)});
    const auto analysis = analyzeWatermarkGaps(utils.createPlan(window));

    ASSERT_EQ(left.getId(), right.getId());
    ASSERT_NE(left, right);
    const auto& sources = analysis->sourcesByWindow.at(window);
    EXPECT_THAT(sources.at(OriginId(1)), ::testing::ElementsAre(OriginId(1)));
    EXPECT_THAT(sources.at(OriginId(2)), ::testing::ElementsAre(OriginId(2)));
}

/// Neither equal operator ids nor structural equality make two separate windows the same DAG node.
TEST_F(TraceSourcesOfInputOriginsTest, SiblingDetectionUsesIdentityRatherThanOperatorIdsOrStructure)
{
    const auto left = withOrigins(utils.createSource("identitySharedLeft", {"a"}), {OriginId(1)});
    const auto right = withOrigins(utils.createSource("identitySharedRight", {"b"}), {OriginId(2)});
    const auto window = withOrigins(join(left, right, field(left, "a"), field(right, "b")), {OriginId(3)});
    const auto clone = window->withOperatorId(window.getId());
    const auto analysis = analyzeWatermarkGaps(utils.createPlan(std::vector<LogicalOperator>{window, clone}));

    ASSERT_EQ(window.getId(), clone.getId());
    ASSERT_NE(window, clone);
    EXPECT_EQ(*window, *clone);
    EXPECT_EQ(analysis->sourcesByWindow.size(), 2);
    EXPECT_THAT(analysis->windowsWithoutThrottling, ::testing::UnorderedElementsAre(window, clone));
    EXPECT_EQ(analysis->sourcesByWindow.at(window), analysis->sourcesByWindow.at(clone));
}

/// Separate plans may reuse operator ids. A new compilation must not reuse provenance cached for an earlier plan.
TEST_F(TraceSourcesOfInputOriginsTest, AnalysesAreIsolatedWhenOperatorIdsAreReused)
{
    const auto left = withOrigins(utils.createSource("isolationLeft", {"a"}), {OriginId(1)});
    const auto right = withOrigins(utils.createSource("isolationRight", {"b"}), {OriginId(2)});
    const auto window = withOrigins(join(left, right, field(left, "a"), field(right, "b")), {OriginId(3)});
    const auto analysis1 = analyzeWatermarkGaps(utils.createPlan(window));
    /// Restore ids through the erased operator API to model separate deserializations with different origin metadata.
    const auto rewrittenLeft = withOrigins(left, {OriginId(101)})->withOperatorId(left.getId());
    const auto rewrittenRight = withOrigins(right, {OriginId(102)})->withOperatorId(right.getId());
    const auto rewritten = window.withChildren({rewrittenLeft, rewrittenRight})->withOperatorId(window.getId());
    const auto analysis2 = analyzeWatermarkGaps(utils.createPlan(rewritten));

    ASSERT_EQ(window.getId(), rewritten.getId());
    ASSERT_NE(window, rewritten);
    EXPECT_THAT(analysis1->sourcesByWindow.at(window).at(OriginId(1)), ::testing::ElementsAre(OriginId(1)));
    EXPECT_THAT(analysis2->sourcesByWindow.at(rewritten).at(OriginId(101)), ::testing::ElementsAre(OriginId(101)));
    EXPECT_FALSE(analysis1->sourcesByWindow.contains(rewritten));
    EXPECT_FALSE(analysis2->sourcesByWindow.contains(window));
    EXPECT_FALSE(analysis2->sourcesByWindow.at(rewritten).contains(OriginId(1)));
    EXPECT_EQ(traceThrottleableSourcesOfInputOrigins(window, analysis1), analysis1->sourcesByWindow.at(window));
    EXPECT_EQ(traceThrottleableSourcesOfInputOrigins(rewritten, analysis2), analysis2->sourcesByWindow.at(rewritten));
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
}
