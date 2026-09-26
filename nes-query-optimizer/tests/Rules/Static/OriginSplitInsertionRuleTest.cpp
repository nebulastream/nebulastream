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

#include <algorithm>
#include <iterator>
#include <ranges>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginSplitLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Static/OriginIdInferenceRule.hpp>
#include <Rules/Static/OriginSplitInsertionRule.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Traits/OriginMappingTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)

class OriginSplitInsertionRuleTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("OriginSplitInsertionRuleTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;

    /// The branch a sink reads through, which the rule is expected to head with a split.
    static LogicalOperator branchOf(const LogicalOperator& sink) { return sink.getChildren().at(0); }

    static std::vector<OriginId> forwardedOriginIds(const LogicalOperator& op)
    {
        const auto originIds = op.getTraitSet().get<OutputOriginIdsTrait>();
        return {originIds->begin(), originIds->end()};
    }
};

/// Two sinks reading one source: each branch is headed by a split of its own, while the source below them stays
/// shared, so the plan keeps one instance of what the branches have in common.
TEST_F(OriginSplitInsertionRuleTest, HeadsEveryBranchOfAFanOutWithItsOwnSplit)
{
    const auto source = utils.createSource("fanOut", {"a", "b"});
    const auto sinkA = utils.createSink(source, "fanOutSinkA", {"a", "b"});
    const auto sinkB = utils.createSink(source, "fanOutSinkB", {"a", "b"});

    const auto rewritten = OriginSplitInsertionRule{}.apply(utils.createPlan({sinkA, sinkB}));

    ASSERT_EQ(rewritten.getRootOperators().size(), 2U);
    const auto firstBranch = branchOf(rewritten.getRootOperators().at(0));
    const auto secondBranch = branchOf(rewritten.getRootOperators().at(1));
    ASSERT_TRUE(firstBranch.tryGetAs<OriginSplitLogicalOperator>().has_value());
    ASSERT_TRUE(secondBranch.tryGetAs<OriginSplitLogicalOperator>().has_value());
    EXPECT_NE(firstBranch, secondBranch) << "both branches were headed by the same split instead of one each";
    EXPECT_EQ(firstBranch.getChildren().at(0), secondBranch.getChildren().at(0))
        << "the operator the branches share was duplicated instead of staying shared";
}

/// An operator read by one consumer keeps its identity, so a plan without a fan-out is left as it is.
TEST_F(OriginSplitInsertionRuleTest, LeavesASingleConsumerChainAlone)
{
    const auto source = utils.createSource("singleConsumer", {"a", "b"});
    const auto sink = utils.createSink(source, "singleConsumerSink", {"a", "b"});

    const auto rewritten = OriginSplitInsertionRule{}.apply(utils.createPlan(sink));

    ASSERT_EQ(rewritten.getRootOperators().size(), 1U);
    EXPECT_FALSE(branchOf(rewritten.getRootOperators().at(0)).tryGetAs<OriginSplitLogicalOperator>().has_value());
}

/// The rule reads the splits already in the plan as branches that have an identity, so applying it twice inserts
/// one split per branch rather than stacking a second one on top.
TEST_F(OriginSplitInsertionRuleTest, InsertsNoSecondSplitWhenRunAgain)
{
    const auto source = utils.createSource("runTwice", {"a", "b"});
    const auto sinkA = utils.createSink(source, "runTwiceSinkA", {"a", "b"});
    const auto sinkB = utils.createSink(source, "runTwiceSinkB", {"a", "b"});

    const OriginSplitInsertionRule rule;
    const auto rewritten = rule.apply(rule.apply(utils.createPlan({sinkA, sinkB})));

    for (const auto& sink : rewritten.getRootOperators())
    {
        const auto branch = branchOf(sink);
        ASSERT_TRUE(branch.tryGetAs<OriginSplitLogicalOperator>().has_value());
        EXPECT_FALSE(branch.getChildren().at(0).tryGetAs<OriginSplitLogicalOperator>().has_value()) << "a split was stacked on a split";
    }
}

/// Every branch stamps ids of its own, one per origin it reads, and no id is carried by two branches.
TEST_F(OriginSplitInsertionRuleTest, GivesEachBranchOriginIdsOfItsOwn)
{
    const auto source = utils.createSource("branchIds", {"a", "b"});
    const auto sinkA = utils.createSink(source, "branchIdsSinkA", {"a", "b"});
    const auto sinkB = utils.createSink(source, "branchIdsSinkB", {"a", "b"});

    const auto rewritten = OriginIdInferenceRule{}.apply(OriginSplitInsertionRule{}.apply(utils.createPlan({sinkA, sinkB})));

    ASSERT_EQ(rewritten.getRootOperators().size(), 2U);
    const auto firstBranch = branchOf(rewritten.getRootOperators().at(0));
    const auto secondBranch = branchOf(rewritten.getRootOperators().at(1));

    const auto sourceOriginIds = forwardedOriginIds(firstBranch.getChildren().at(0));
    ASSERT_EQ(sourceOriginIds.size(), 1U);

    for (const auto& branch : {firstBranch, secondBranch})
    {
        const auto mapping = branch.getTraitSet().get<OriginMappingTrait>()->originMapping;
        ASSERT_EQ(mapping.size(), sourceOriginIds.size()) << "a branch must map every origin it reads";
        EXPECT_EQ(mapping.at(0).first, sourceOriginIds.at(0));
        EXPECT_NE(mapping.at(0).second, sourceOriginIds.at(0)) << "the branch forwards the id it read instead of one of its own";
        EXPECT_EQ(forwardedOriginIds(branch), std::vector{mapping.at(0).second});
    }

    EXPECT_NE(forwardedOriginIds(firstBranch).at(0), forwardedOriginIds(secondBranch).at(0)) << "both branches carry the same origin id";
}

/// A fork that merges again: the operator where the branches meet reads two streams, and it must see them under
/// two origin ids. Sequence numbers are unique only within an origin, so one id for both would make the two
/// streams indistinguishable there — which is why the identity is decided at the fan-out and not at a node
/// boundary that this plan does not even have.
TEST_F(OriginSplitInsertionRuleTest, TellsTheBranchesOfADiamondApart)
{
    const auto source = utils.createSource("diamond", {"a", "b"});
    const auto merged = UnionLogicalOperator::create().withChildren({source, source});
    const auto sink = utils.createSink(merged, "diamondSink", {"a", "b"});

    const auto rewritten = OriginIdInferenceRule{}.apply(OriginSplitInsertionRule{}.apply(utils.createPlan(sink)));

    ASSERT_EQ(rewritten.getRootOperators().size(), 1U);
    const auto mergePoint = branchOf(rewritten.getRootOperators().at(0));
    ASSERT_EQ(mergePoint.getChildren().size(), 2U);
    for (const auto& branch : mergePoint.getChildren())
    {
        EXPECT_TRUE(branch.tryGetAs<OriginSplitLogicalOperator>().has_value());
    }

    const auto mergedOriginIds = forwardedOriginIds(mergePoint);
    ASSERT_EQ(mergedOriginIds.size(), 2U) << "the branches reached the merge point under one origin id";
    EXPECT_NE(mergedOriginIds.at(0), mergedOriginIds.at(1));
}

/// A fan-out inside a branch of another fan-out: the union reads the source twice, and both sinks read the union.
/// The splits of the upper fan-out therefore read what the splits of the lower one stamp, and the mappings have to
/// chain — an upper branch that replaced the id the source read would hand its stream an identity the other upper
/// branch carries as well. All four branches sit on one node here, so this also pins that several splits below one
/// another do not interfere.
TEST_F(OriginSplitInsertionRuleTest, ChainsTheMappingsOfNestedFanOuts)
{
    const auto source = utils.createSource("nested", {"a", "b"});
    const auto merged = UnionLogicalOperator::create().withChildren({source, source});
    const auto sinkA = utils.createSink(merged, "nestedSinkA", {"a", "b"});
    const auto sinkB = utils.createSink(merged, "nestedSinkB", {"a", "b"});

    const auto rewritten = OriginIdInferenceRule{}.apply(OriginSplitInsertionRule{}.apply(utils.createPlan({sinkA, sinkB})));

    ASSERT_EQ(rewritten.getRootOperators().size(), 2U);
    const std::vector upperBranches{branchOf(rewritten.getRootOperators().at(0)), branchOf(rewritten.getRootOperators().at(1))};
    for (const auto& branch : upperBranches)
    {
        ASSERT_TRUE(branch.tryGetAs<OriginSplitLogicalOperator>().has_value());
        ASSERT_EQ(branch.getChildren().size(), 1U);
    }

    const auto mergePoint = upperBranches.at(0).getChildren().at(0);
    EXPECT_EQ(mergePoint, upperBranches.at(1).getChildren().at(0)) << "the operator the upper branches share was copied per branch";
    ASSERT_EQ(mergePoint.getChildren().size(), 2U);

    std::vector<OriginId> lowerIds;
    for (const auto& lowerBranch : mergePoint.getChildren())
    {
        ASSERT_TRUE(lowerBranch.tryGetAs<OriginSplitLogicalOperator>().has_value());
        std::ranges::copy(forwardedOriginIds(lowerBranch), std::back_inserter(lowerIds));
    }
    std::ranges::sort(lowerIds);
    ASSERT_EQ(lowerIds.size(), 2U) << "the lower fan-out did not give both of its branches an identity";

    auto branchIds = lowerIds;
    for (const auto& branch : upperBranches)
    {
        auto replacedIds = branch.getTraitSet().get<OriginMappingTrait>()->originMapping
            | std::views::transform([](const auto& mapping) { return mapping.first; }) | std::ranges::to<std::vector>();
        std::ranges::sort(replacedIds);
        EXPECT_EQ(replacedIds, lowerIds) << "an upper branch replaces ids that the splits below it do not stamp";
        std::ranges::copy(forwardedOriginIds(branch), std::back_inserter(branchIds));
    }

    std::ranges::sort(branchIds);
    EXPECT_TRUE(std::ranges::adjacent_find(branchIds) == branchIds.end()) << "two branches of the plan carry the same origin id";
}

/// Each node lowers the fragment it was sent, so the branch identities have to survive the way there.
TEST_F(OriginSplitInsertionRuleTest, SurvivesSerialization)
{
    const auto source = utils.createSource("serialized", {"a", "b"});
    const auto sinkA = utils.createSink(source, "serializedSinkA", {"a", "b"});
    const auto sinkB = utils.createSink(source, "serializedSinkB", {"a", "b"});
    const auto rewritten = OriginIdInferenceRule{}.apply(OriginSplitInsertionRule{}.apply(utils.createPlan({sinkA, sinkB})));

    const auto restored = QueryPlanSerializationUtil::deserializeQueryPlan(QueryPlanSerializationUtil::serializeQueryPlan(rewritten));

    const auto splits = getOperatorByType<OriginSplitLogicalOperator>(restored);
    ASSERT_EQ(splits.size(), 2U);
    for (const auto& split : splits)
    {
        const auto mapping = split.getTraitSet().get<OriginMappingTrait>()->originMapping;
        ASSERT_EQ(mapping.size(), 1U);
        EXPECT_NE(mapping.at(0).first, mapping.at(0).second);
    }
    EXPECT_NE(
        splits.at(0).getTraitSet().get<OriginMappingTrait>()->originMapping.at(0).second,
        splits.at(1).getTraitSet().get<OriginMappingTrait>()->originMapping.at(0).second);
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
