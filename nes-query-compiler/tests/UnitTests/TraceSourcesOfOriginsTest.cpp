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

#include <Phases/TraceSourcesOfOrigins.hpp>

#include <string>
#include <utility>
#include <vector>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
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
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;

/// The single origin that the origin inference assigned to `op`.
OriginId originOf(const LogicalOperator& op)
{
    const auto origins = op.getTraitSet().get<OutputOriginIdsTrait>();
    EXPECT_EQ(origins->size(), 1);
    return (*origins)[0];
}

LogicalOperator withOrigins(const LogicalOperator& op, std::vector<OriginId> origins)
{
    auto traitSet = op.getTraitSet();
    EXPECT_TRUE(tryInsert(traitSet, OutputOriginIdsTrait{std::move(origins)}));
    return op.withTraitSet(traitSet);
}

LogicalOperator join(const LogicalOperator& left, const std::string& leftField, const LogicalOperator& right, const std::string& rightField)
{
    return JoinLogicalOperator::create(
        {left, right},
        EqualsLogicalFunction{
            FieldAccessLogicalFunction{left.getOutputSchema().getFieldByName(Identifier::parse(leftField)).value()},
            FieldAccessLogicalFunction{right.getOutputSchema().getFieldByName(Identifier::parse(rightField)).value()}},
        Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{0}}},
        JoinLogicalOperator::JoinType::INNER_JOIN,
        JoinTimeCharacteristic{});
}
}

class TraceSourcesOfOriginsTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("TraceSourcesOfOriginsTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
};

/// J2 joins the output of J1 with a third source, so the inputs of J2 are an operator origin and a source.
TEST_F(TraceSourcesOfOriginsTest, JoinOverJoin)
{
    const auto source1 = utils.createSource("stacked1", {"a", "b"});
    const auto source2 = utils.createSource("stacked2", {"c", "d"});
    const auto source3 = utils.createSource("stacked3", {"e", "f"});
    const auto join1 = join(
        IngestionTimeWatermarkAssignerLogicalOperator::create(source1),
        "a",
        IngestionTimeWatermarkAssignerLogicalOperator::create(source2),
        "c");
    const std::vector<std::pair<Identifier, LogicalFunction>> projections{
        {Identifier::parse("a"), FieldAccessLogicalFunction{join1.getOutputSchema()[Identifier::parse("a")].value()}},
        {Identifier::parse("b"), FieldAccessLogicalFunction{join1.getOutputSchema()[Identifier::parse("b")].value()}}};
    const auto projection = ProjectionLogicalOperator::create(join1, projections, ProjectionLogicalOperator::Asterisk{false});
    const auto join2 = join(projection, "a", IngestionTimeWatermarkAssignerLogicalOperator::create(source3), "e");
    const auto plan
        = OriginIdInferenceRule{}.apply(utils.createPlan(utils.createSink(join2, "stacked", {"a", "b", "e", "f", "start", "end"})));

    const auto j2 = plan.getRootOperators().at(0).getChildren().at(0);
    const auto j1 = j2.getChildren().at(0).getChildren().at(0);
    const auto s1 = originOf(j1.getChildren().at(0).getChildren().at(0));
    const auto s2 = originOf(j1.getChildren().at(1).getChildren().at(0));
    const auto s3 = originOf(j2.getChildren().at(1).getChildren().at(0));
    const auto [sourcesOfOrigins, inputsOfOrigins] = traceOrigins(plan);

    EXPECT_THAT(sourcesOfOrigins.at(originOf(j2)), UnorderedElementsAre(s1, s2, s3));
    EXPECT_THAT(sourcesOfOrigins.at(originOf(j1)), UnorderedElementsAre(s1, s2));
    for (const auto source : {s1, s2, s3})
    {
        EXPECT_THAT(sourcesOfOrigins.at(source), UnorderedElementsAre(source));
        EXPECT_FALSE(inputsOfOrigins.contains(source));
    }
    EXPECT_THAT(inputsOfOrigins.at(originOf(j2)), UnorderedElementsAre(originOf(j1), s3));
    EXPECT_THAT(inputsOfOrigins.at(originOf(j1)), UnorderedElementsAre(s1, s2));
    EXPECT_EQ(inputsOfOrigins.size(), 2);
}

/// A union does not assign an origin, so the join sees the origins of both sources under the union as its inputs.
TEST_F(TraceSourcesOfOriginsTest, UnionUnderAWindow)
{
    const auto source1 = utils.createSource("union1", {"a", "b"});
    const auto source2 = utils.createSource("union2", {"a", "b"});
    const auto source3 = utils.createSource("union3", {"e", "f"});
    const auto unioned = UnionLogicalOperator::create(
        {IngestionTimeWatermarkAssignerLogicalOperator::create(source1), IngestionTimeWatermarkAssignerLogicalOperator::create(source2)});
    const auto joined = join(unioned, "a", IngestionTimeWatermarkAssignerLogicalOperator::create(source3), "e");
    const auto plan
        = OriginIdInferenceRule{}.apply(utils.createPlan(utils.createSink(joined, "union", {"a", "b", "e", "f", "start", "end"})));

    const auto j = plan.getRootOperators().at(0).getChildren().at(0);
    const auto u = j.getChildren().at(0);
    const auto s1 = originOf(u.getChildren().at(0).getChildren().at(0));
    const auto s2 = originOf(u.getChildren().at(1).getChildren().at(0));
    const auto s3 = originOf(j.getChildren().at(1).getChildren().at(0));
    const auto [sourcesOfOrigins, inputsOfOrigins] = traceOrigins(plan);

    EXPECT_THAT(sourcesOfOrigins.at(originOf(j)), UnorderedElementsAre(s1, s2, s3));
    EXPECT_THAT(inputsOfOrigins.at(originOf(j)), UnorderedElementsAre(s1, s2, s3));
    EXPECT_EQ(inputsOfOrigins.size(), 1);
}

/// A network source carries every origin of its upstream operator, and all of them map to the network source.
/// The network source is identified by its first origin, as LowerToPhysicalSource lowers it with that origin.
TEST_F(TraceSourcesOfOriginsTest, NetworkSourceWithSeveralOrigins)
{
    constexpr OriginId upstream1{7};
    constexpr OriginId upstream2{8};
    constexpr OriginId local{9};
    constexpr OriginId joinOutput{10};
    const auto network = withOrigins(utils.createSource("network", {"a", "b"}), {upstream1, upstream2});
    const auto source = withOrigins(utils.createSource("local", {"e", "f"}), {local});
    const auto joined = withOrigins(join(network, "a", source, "e"), {joinOutput});
    const auto plan
        = utils.createPlan(withOrigins(utils.createSink(joined, "network", {"a", "b", "e", "f", "start", "end"}), {joinOutput}));

    const auto [sourcesOfOrigins, inputsOfOrigins] = traceOrigins(plan);

    EXPECT_THAT(sourcesOfOrigins.at(upstream1), UnorderedElementsAre(upstream1));
    EXPECT_THAT(sourcesOfOrigins.at(upstream2), UnorderedElementsAre(upstream1));
    EXPECT_THAT(sourcesOfOrigins.at(local), UnorderedElementsAre(local));
    EXPECT_THAT(sourcesOfOrigins.at(joinOutput), UnorderedElementsAre(upstream1, local));
    EXPECT_THAT(inputsOfOrigins.at(joinOutput), UnorderedElementsAre(upstream1, upstream2, local));
    EXPECT_EQ(inputsOfOrigins.size(), 1);
}

/// Without a window-based operator, no origin has inputs, so no source is on a critical path.
TEST_F(TraceSourcesOfOriginsTest, PlanWithoutWindowsHasNoInputs)
{
    const auto source = utils.createSource("plain", {"a", "b"});
    const auto plan = OriginIdInferenceRule{}.apply(utils.createPlan(utils.createSink(source, "plain", {"a", "b"})));

    const auto [sourcesOfOrigins, inputsOfOrigins] = traceOrigins(plan);

    const auto s = originOf(plan.getRootOperators().at(0).getChildren().at(0));
    EXPECT_THAT(sourcesOfOrigins.at(s), UnorderedElementsAre(s));
    EXPECT_THAT(inputsOfOrigins, IsEmpty());
}

}
