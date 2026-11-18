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

#include <cstddef>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ChangeLog.hpp>
#include <OperatorGraph.hpp>
#include <OptimizerTestUtils.hpp>
#include <QueryId.hpp>
#include <SharedQueryPlan.hpp>

/// NOLINTBEGIN(readability-magic-numbers)
namespace NES
{

using namespace ::testing;

/// Uses the example plan of the original's `computeDecomposedQueryPlans`:
///
/// OP1 --> OP2 -----------------> OP7 --> OP8 --> OP9
///            \                 /
///             -> OP5 --> OP6 --
///            /                 \
/// OP3 --> OP4                   --> OP10
///
/// The graph only depends on the parent/child relation, so the leafs are logical sources and all other operators are unions.
class SharedQueryPlanTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SharedQueryPlanTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        auto op1 = source("s1");
        auto op3 = source("s3");
        auto op2 = node({op1});
        auto op4 = node({op3});
        auto op5 = node({op2, op4});
        auto op6 = node({op5});
        auto op7 = node({op2, op6});
        auto op8 = node({op7});
        auto op9 = node({op8});
        auto op10 = node({op6});
        for (const auto& op : {op1, op2, op3, op4, op5, op6, op7, op8, op9, op10})
        {
            ids.emplace_back(op.getId());
        }
        plan.emplace(QueryId::createDistributed(DistributedQueryId("q")), std::vector<LogicalOperator>{op9, op10});
    }

    static LogicalOperator node(std::vector<LogicalOperator> children) { return UnionLogicalOperator::create(std::move(children)); }

    LogicalOperator source(const std::string& name) { return utils.createSource(name, {"a"}); }

    /// Operator id of OPx
    [[nodiscard]] OperatorId op(const size_t number) const { return ids.at(number - 1); }

    [[nodiscard]] std::set<OperatorId> ops(const std::vector<size_t>& numbers) const
    {
        std::set<OperatorId> result;
        for (const auto number : numbers)
        {
            result.insert(op(number));
        }
        return result;
    }

    [[nodiscard]] std::unordered_map<OperatorId, Host> placeAllOn(const Host& host) const
    {
        std::unordered_map<OperatorId, Host> placement;
        for (const auto& id : ids)
        {
            placement.emplace(id, host);
        }
        return placement;
    }

    OptimizerTestUtils utils;
    std::vector<OperatorId> ids;
    std::optional<LogicalPlan> plan;
};

TEST_F(SharedQueryPlanTest, OperatorGraphNavigation)
{
    const OperatorGraph graph(*plan);
    EXPECT_THAT(graph.getAllOperators(), SizeIs(10));
    EXPECT_THAT(graph.getRootOperators(), UnorderedElementsAre(op(9), op(10)));
    EXPECT_THAT(graph.getLeafOperators(), UnorderedElementsAre(op(1), op(3)));
    EXPECT_THAT(graph.getDownstreamOperators(op(2)), UnorderedElementsAre(op(5), op(7)));
    EXPECT_THAT(graph.getUpstreamOperators(op(5)), UnorderedElementsAre(op(2), op(4)));

    EXPECT_TRUE(graph.isDownstreamOf(op(9), op(1)));
    EXPECT_TRUE(graph.isDownstreamOf(op(10), op(1)));
    EXPECT_FALSE(graph.isDownstreamOf(op(1), op(9)));
    EXPECT_FALSE(graph.isDownstreamOf(op(10), op(7)));
    EXPECT_FALSE(graph.isDownstreamOf(op(1), op(1)));
}

TEST_F(SharedQueryPlanTest, FindAllOperatorsBetween)
{
    const OperatorGraph graph(*plan);
    EXPECT_THAT(graph.findAllOperatorsBetween(ops({7}), ops({1})), ContainerEq(ops({2, 5, 6})));
    EXPECT_THAT(graph.findAllOperatorsBetween(ops({9, 10}), ops({2, 4})), ContainerEq(ops({5, 6, 7, 8})));
    EXPECT_THAT(graph.findAllOperatorsBetween(ops({4}), ops({1})), IsEmpty());
}

/// With more than one downstream operator, only the paths towards the downstream operators of the entry belong to the poset.
TEST_F(SharedQueryPlanTest, ChangeLogEntryPoSet)
{
    const OperatorGraph graph(*plan);
    EXPECT_THAT(ChangeLogEntry::create(ops({1}), ops({7}), graph).poSetOfSubQueryPlan, ContainerEq(ops({1, 2, 5, 6, 7})));
    EXPECT_THAT(ChangeLogEntry::create(ops({3}), ops({4}), graph).poSetOfSubQueryPlan, ContainerEq(ops({3, 4})));
    EXPECT_THAT(ChangeLogEntry::create({}, ops({4}), graph).poSetOfSubQueryPlan, IsEmpty());
}

/// Overlapping entries are merged into one entry with the most upstream and most downstream operators, disjoint entries stay.
TEST_F(SharedQueryPlanTest, ChangeLogCompaction)
{
    const OperatorGraph graph(*plan);
    ChangeLog changeLog;
    const auto first = changeLog.addChangeLogEntry(ChangeLogEntry::create(ops({1}), ops({5}), graph));
    changeLog.addChangeLogEntry(ChangeLogEntry::create(ops({3}), ops({4}), graph));
    changeLog.addChangeLogEntry(ChangeLogEntry::create(ops({2}), ops({7}), graph));
    const auto now = changeLog.getCurrentTimestamp();
    /// Entries added after the timestamp are neither compacted nor returned.
    changeLog.addChangeLogEntry(ChangeLogEntry::create(ops({8}), ops({9}), graph));

    const auto entries = changeLog.getCompactChangeLogEntriesBeforeTimestamp(now, graph);
    ASSERT_THAT(entries, SizeIs(2));
    EXPECT_THAT(entries.at(0).second.upstreamOperators, ContainerEq(ops({1})));
    EXPECT_THAT(entries.at(0).second.downstreamOperators, ContainerEq(ops({7})));
    EXPECT_THAT(entries.at(0).first, Eq(first));
    EXPECT_THAT(entries.at(1).second.upstreamOperators, ContainerEq(ops({3})));

    changeLog.updateProcessedChangeLogTimestamp(now);
    EXPECT_THAT(changeLog.getChangeLogEntriesBeforeTimestamp(changeLog.getCurrentTimestamp()), SizeIs(1));
}

TEST_F(SharedQueryPlanTest, CompactChainOfEntries)
{
    const OperatorGraph graph(*plan);
    const auto compacted = ChangeLog::compactChangeLogEntries(
        {ChangeLogEntry::create(ops({2}), ops({7}), graph),
         ChangeLogEntry::create(ops({1}), ops({5}), graph),
         ChangeLogEntry::create(ops({5}), ops({9}), graph)},
        graph);
    EXPECT_THAT(compacted.upstreamOperators, ContainerEq(ops({1})));
    EXPECT_THAT(compacted.downstreamOperators, ContainerEq(ops({9})));
}

TEST_F(SharedQueryPlanTest, NewQueryHasOneEntryCoveringThePlan)
{
    SharedQueryPlan sharedQueryPlan(DistributedQueryId("q"), *plan);
    EXPECT_THAT(sharedQueryPlan.getStatus(), Eq(SharedQueryPlanStatus::Created));
    EXPECT_THAT(sharedQueryPlan.getOperatorState(op(5)), Eq(OperatorState::ToBePlaced));
    const auto entries = sharedQueryPlan.getChangeLogEntries(sharedQueryPlan.getCurrentChangeLogTimestamp());
    ASSERT_THAT(entries, SizeIs(1));
    EXPECT_THAT(entries.front().second.upstreamOperators, ContainerEq(ops({1, 3})));
    EXPECT_THAT(entries.front().second.downstreamOperators, ContainerEq(ops({9, 10})));
    EXPECT_THAT(entries.front().second.poSetOfSubQueryPlan, SizeIs(10));
}

TEST_F(SharedQueryPlanTest, ReOperatorPlacement)
{
    SharedQueryPlan sharedQueryPlan(DistributedQueryId("q"), *plan, placeAllOn(Host("worker")));
    EXPECT_THAT(sharedQueryPlan.getStatus(), Eq(SharedQueryPlanStatus::Deployed));
    EXPECT_THAT(sharedQueryPlan.getPinnedWorker(op(5)), ::testing::Optional(Host("worker")));
    EXPECT_THAT(sharedQueryPlan.getChangeLogEntries(sharedQueryPlan.getCurrentChangeLogTimestamp()), IsEmpty());

    sharedQueryPlan.performReOperatorPlacement(ops({1}), ops({7}));
    for (const auto number : {2, 5, 6})
    {
        EXPECT_THAT(sharedQueryPlan.getOperatorState(op(number)), Eq(OperatorState::ToBeReplaced)) << "OP" << number;
    }
    for (const auto number : {1, 3, 4, 7, 8, 9, 10})
    {
        EXPECT_THAT(sharedQueryPlan.getOperatorState(op(number)), Eq(OperatorState::Placed)) << "OP" << number;
    }
    const auto entries = sharedQueryPlan.getChangeLogEntries(sharedQueryPlan.getCurrentChangeLogTimestamp());
    ASSERT_THAT(entries, SizeIs(1));
    EXPECT_THAT(entries.front().second.poSetOfSubQueryPlan, ContainerEq(ops({1, 2, 5, 6, 7})));
}

/// A failed entry is recorded again between the operators that are adjacent to the operators which are not placed.
TEST_F(SharedQueryPlanTest, RecordFailedChangeLogEntries)
{
    SharedQueryPlan sharedQueryPlan(DistributedQueryId("q"), *plan, placeAllOn(Host("worker")));
    sharedQueryPlan.setOperatorState(op(5), OperatorState::ToBeReplaced);
    sharedQueryPlan.setOperatorState(op(6), OperatorState::ToBeReplaced);

    const OperatorGraph graph(*plan);
    sharedQueryPlan.recordFailedChangeLogEntries({ChangeLogEntry::create(ops({1}), ops({9}), graph)});
    const auto entries = sharedQueryPlan.getChangeLogEntries(sharedQueryPlan.getCurrentChangeLogTimestamp());
    ASSERT_THAT(entries, SizeIs(1));
    EXPECT_THAT(entries.front().second.upstreamOperators, ContainerEq(ops({2})));
    EXPECT_THAT(entries.front().second.downstreamOperators, ContainerEq(ops({7})));
}

}

/// NOLINTEND(readability-magic-numbers)
