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
#include <ranges>
#include <sstream>

#include <unordered_map>
#include <utility>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Debug/DebugHelpers.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/Trait.hpp>
#include <Util/Reflection.hpp>
#include <Util/UUID.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <QueryId.hpp>

using namespace NES;

QueryId randomQueryId()
{
    return QueryId::createLocal(LocalQueryId(generateUUID()));
}

class LogicalPlanTest : public ::testing::Test
{
protected:
    LogicalPlanTest()
        : sourceOp{SourceNameLogicalOperator("Source")}
        , sourceOp2(
              [this]
              {
                  const auto dummySchema = Schema{};
                  const auto logicalSource = sourceCatalog.addLogicalSource("Source", dummySchema).value();
                  const std::unordered_map<std::string, std::string> dummyParserConfig
                      = {{"type", "CSV"}, {"tuple_delimiter", "\n"}, {"field_delimiter", ","}};
                  auto dummySourceDescriptor
                      = sourceCatalog
                            .addPhysicalSource(logicalSource, "File", Host("localhost"), {{"file_path", "/dev/null"}}, dummyParserConfig)
                            .value();
                  return LogicalOperator{SourceDescriptorLogicalOperator(std::move(dummySourceDescriptor))};
              }())
        , selectionOp{SelectionLogicalOperator(FieldAccessLogicalFunction("logicalfunction"))}
        , sinkOp{SinkLogicalOperator()}
    {
    }

    SourceCatalog sourceCatalog;
    LogicalOperator sourceOp;
    LogicalOperator sourceOp2;
    LogicalOperator selectionOp;
    LogicalOperator sinkOp;
};

TEST_F(LogicalPlanTest, SingleRootConstructor)
{
    const LogicalPlan plan(INVALID_QUERY_ID, {sourceOp});
    EXPECT_EQ(plan.getRootOperators().size(), 1);
    EXPECT_EQ(plan.getRootOperators()[0], sourceOp);
}

TEST_F(LogicalPlanTest, MultipleRootsConstructor)
{
    const std::vector roots = {sourceOp, selectionOp};
    const auto queryId = randomQueryId();
    LogicalPlan plan(queryId, roots);
    EXPECT_EQ(plan.getRootOperators().size(), 2);
    EXPECT_EQ(plan.getQueryId(), queryId);
    EXPECT_EQ(plan.getRootOperators()[0], sourceOp);
    EXPECT_EQ(plan.getRootOperators()[1], selectionOp);
}

TEST_F(LogicalPlanTest, CopyConstructor)
{
    const LogicalPlan original(INVALID_QUERY_ID, {sourceOp});
    const LogicalPlan& copy(original);
    EXPECT_EQ(copy.getRootOperators().size(), 1);
    EXPECT_EQ(copy.getRootOperators()[0], sourceOp);
}

TEST_F(LogicalPlanTest, PromoteOperatorToRoot)
{
    const LogicalOperator sourceOp{SourceNameLogicalOperator("source")};
    const LogicalOperator selectionOp{SelectionLogicalOperator(FieldAccessLogicalFunction("field"))};
    const auto plan = LogicalPlan(INVALID_QUERY_ID, {sourceOp});
    const auto promoteResultPlan = promoteOperatorToRoot(plan, selectionOp);
    EXPECT_EQ(*promoteResultPlan.getRootOperators()[0], *selectionOp);
    EXPECT_EQ(*promoteResultPlan.getRootOperators()[0].getChildren()[0], *sourceOp);
}

TEST_F(LogicalPlanTest, ReplaceOperator)
{
    const LogicalOperator sourceOp{SourceNameLogicalOperator("source")};
    const LogicalOperator sourceOp2{SourceNameLogicalOperator("source2")};
    const auto plan = LogicalPlan(INVALID_QUERY_ID, {sourceOp});
    const auto result = replaceOperator(plan, sourceOp.getId(), sourceOp2);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(*result->getRootOperators()[0], *sourceOp2); ///NOLINT(bugprone-unchecked-optional-access)
}

TEST_F(LogicalPlanTest, replaceSubtree)
{
    const LogicalOperator sourceOp{SourceNameLogicalOperator("source")};
    const LogicalOperator sourceOp2{SourceNameLogicalOperator("source2")};
    const auto plan = LogicalPlan(INVALID_QUERY_ID, {sourceOp});
    const auto result = replaceSubtree(plan, sourceOp.getId(), sourceOp2);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->getRootOperators()[0].getId(), sourceOp2.getId()); ///NOLINT(bugprone-unchecked-optional-access)
}

TEST_F(LogicalPlanTest, GetParents)
{
    const LogicalOperator sourceOp{SourceNameLogicalOperator("source")};
    const LogicalOperator selectionOp{SelectionLogicalOperator(FieldAccessLogicalFunction("field"))};
    const auto plan = LogicalPlan(INVALID_QUERY_ID, {sourceOp});
    const auto promoteResult = promoteOperatorToRoot(plan, selectionOp);
    const auto parents = getParents(promoteResult, sourceOp);
    EXPECT_EQ(parents.size(), 1);
    EXPECT_EQ(*parents[0], *selectionOp);
}

TEST_F(LogicalPlanTest, GetOperatorByType)
{
    const auto sourceOp = LogicalOperator{SourceNameLogicalOperator("source")};
    const auto plan = LogicalPlan(INVALID_QUERY_ID, {sourceOp});
    const auto sourceOperators = getOperatorByType<SourceNameLogicalOperator>(plan);
    EXPECT_EQ(sourceOperators.size(), 1);
    EXPECT_EQ(*LogicalOperator{sourceOperators[0]}, *sourceOp);
}

TEST_F(LogicalPlanTest, GetOperatorsById)
{
    const LogicalOperator sourceOp{SourceNameLogicalOperator{"TestSource"}};
    const LogicalOperator selectionOp{SelectionLogicalOperator{FieldAccessLogicalFunction{"fn"}}.withChildren({sourceOp})};
    const LogicalOperator sinkOp{SinkLogicalOperator{"TestSink"}.withChildren({selectionOp})};
    const LogicalPlan plan(INVALID_QUERY_ID, {sinkOp});
    const auto op1 = getOperatorById(plan, sourceOp.getId());
    EXPECT_TRUE(op1.has_value());
    ///NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_EQ(op1.value().getId(), sourceOp.getId());
    const auto op2 = getOperatorById(plan, selectionOp.getId());
    EXPECT_TRUE(op2.has_value());
    EXPECT_EQ(op2.value().getId(), selectionOp.getId());
    const auto op3 = getOperatorById(plan, sinkOp.getId());
    EXPECT_TRUE(op3.has_value());
    EXPECT_EQ(op3.value().getId(), sinkOp.getId());
}

namespace
{
struct TestTrait final : DefaultTrait<TestTrait>
{
    static constexpr std::string_view NAME = "TestTrait";

    [[nodiscard]] std::string_view getName() const override { return NAME; }
};
}

template <>
struct Reflector<TestTrait>
{
    Reflected operator()(const TestTrait&) const { return Reflected{}; };
};

template <>
struct Unreflector<TestTrait>
{
    TestTrait operator()(const Reflected&) const { return TestTrait{}; };
};

TEST_F(LogicalPlanTest, AddTraits)
{
    EXPECT_TRUE(std::ranges::empty(sourceOp.getTraitSet()));
    const auto sourceWithTrait = sourceOp.withTraitSet(TraitSet{TestTrait{}});
    auto sourceTraitSet = sourceWithTrait.getTraitSet();
    EXPECT_EQ(std::ranges::size(sourceTraitSet), 1);
    EXPECT_TRUE(sourceTraitSet.tryGet<TestTrait>().has_value());
}

TEST_F(LogicalPlanTest, GetLeafOperators)
{
    /// source1 -> selection -> source2
    std::vector children = {sourceOp2};
    selectionOp = selectionOp.withChildren(children);
    children = {selectionOp};
    sourceOp = sourceOp.withChildren(children);
    const LogicalPlan plan(INVALID_QUERY_ID, {sourceOp});
    auto leafOperators = getLeafOperators(plan);
    EXPECT_EQ(leafOperators.size(), 1);
    EXPECT_EQ(leafOperators[0], sourceOp2);
}

TEST_F(LogicalPlanTest, GetAllOperators)
{
    /// source1 -> selection -> source2
    std::vector children = {sourceOp2};
    selectionOp = selectionOp.withChildren(children);
    children = {selectionOp};
    sourceOp = sourceOp.withChildren(children);
    const LogicalPlan plan(INVALID_QUERY_ID, {sourceOp});
    const auto allOperators = flatten(plan);
    EXPECT_EQ(allOperators.size(), 3);
    EXPECT_TRUE(allOperators.contains(sourceOp));
    EXPECT_TRUE(allOperators.contains(selectionOp));
    EXPECT_TRUE(allOperators.contains(sourceOp2));
}

TEST_F(LogicalPlanTest, EqualityOperator)
{
    const LogicalPlan plan1(INVALID_QUERY_ID, {sourceOp});
    const LogicalPlan plan2(INVALID_QUERY_ID, {sourceOp});
    EXPECT_TRUE(plan1 == plan2);

    const LogicalPlan plan3(INVALID_QUERY_ID, {selectionOp});
    EXPECT_FALSE(plan1 == plan3);
}

TEST_F(LogicalPlanTest, OutputOperator)
{
    const LogicalPlan plan(INVALID_QUERY_ID, {sourceOp});
    std::stringstream ss;
    ss << plan;
    EXPECT_FALSE(ss.str().empty());
}

/// ── Breakpoint-target tests ──────────────────────────────────────────────────
/// Build a plan, pause at the marked line, and inspect `plan` or
/// `NES::Debug::view(plan)` in the debugger Variables panel.

TEST_F(LogicalPlanTest, DebugView_DeepFilterPipeline)
{
    /// Source → WatermarkAssigner → Selection(temperature > limit) → Selection(id) → Sink
    const LogicalOperator source{SourceNameLogicalOperator{"sensors"}};
    const LogicalOperator watermark{IngestionTimeWatermarkAssignerLogicalOperator{}.withChildren({source})};
    const LogicalOperator filter1{
        SelectionLogicalOperator{
            GreaterLogicalFunction{FieldAccessLogicalFunction{"temperature"}, FieldAccessLogicalFunction{"limit"}}
        }.withChildren({watermark})};
    const LogicalOperator filter2{SelectionLogicalOperator{FieldAccessLogicalFunction{"id"}}.withChildren({filter1})};
    const LogicalOperator sink{SinkLogicalOperator{"ResultSink"}.withChildren({filter2})};
    const LogicalPlan plan(INVALID_QUERY_ID, {sink});
    EXPECT_EQ(flatten(plan).size(), 5); // <-- set breakpoint here, inspect 'plan'
}

TEST_F(LogicalPlanTest, DebugView_MultiSourceUnion)
{
    /// Source("sensorA") + Source("sensorB") → Union → Selection(active) → Sink
    const LogicalOperator sourceA{SourceNameLogicalOperator{"sensorA"}};
    const LogicalOperator sourceB{SourceNameLogicalOperator{"sensorB"}};
    const LogicalOperator unionOp{UnionLogicalOperator{}.withChildren({sourceA, sourceB})};
    const LogicalOperator selection{SelectionLogicalOperator{FieldAccessLogicalFunction{"active"}}.withChildren({unionOp})};
    const LogicalOperator sink{SinkLogicalOperator{"MergedSink"}.withChildren({selection})};
    const LogicalPlan plan(INVALID_QUERY_ID, {sink});
    EXPECT_EQ(flatten(plan).size(), 5); // <-- set breakpoint here, inspect 'plan'
}

TEST_F(LogicalPlanTest, DebugView_CompoundPredicate)
{
    /// Source → Selection((temperature > limit AND pressure < ceiling) OR active) → Sink
    const LogicalOperator source{SourceNameLogicalOperator{"events"}};
    const LogicalOperator selection{
        SelectionLogicalOperator{
            OrLogicalFunction{
                AndLogicalFunction{
                    GreaterLogicalFunction{FieldAccessLogicalFunction{"temperature"}, FieldAccessLogicalFunction{"limit"}},
                    LessLogicalFunction{FieldAccessLogicalFunction{"pressure"}, FieldAccessLogicalFunction{"ceiling"}}
                },
                FieldAccessLogicalFunction{"active"}
            }
        }.withChildren({source})};
    const LogicalOperator sink{SinkLogicalOperator{"AlarmSink"}.withChildren({selection})};
    const LogicalPlan plan(INVALID_QUERY_ID, {sink});
    EXPECT_EQ(flatten(plan).size(), 3); // <-- set breakpoint here, inspect 'plan'
}

TEST_F(LogicalPlanTest, DebugView_TumblingWindowAggregation)
{
    /// Source("metrics") → WatermarkAssigner → WindowedAggregation(SUM(value) GROUP BY sensor, 5s) → Sink
    const LogicalOperator source{SourceNameLogicalOperator{"metrics"}};
    const LogicalOperator watermark{IngestionTimeWatermarkAssignerLogicalOperator{}.withChildren({source})};
    auto windowType = std::make_shared<Windowing::TumblingWindow>(
        Windowing::TimeCharacteristic::createIngestionTime(),
        Windowing::TimeMeasure(5000));
    auto sumAgg = std::make_shared<WindowAggregationLogicalFunction>(
        SumAggregationLogicalFunction{FieldAccessLogicalFunction{"value"}, FieldAccessLogicalFunction{"sum_value"}});
    const LogicalOperator windowAgg{
        WindowedAggregationLogicalOperator{
            {FieldAccessLogicalFunction{"sensor"}},
            std::vector{sumAgg},
            windowType
        }.withChildren({watermark})};
    const LogicalOperator sink{SinkLogicalOperator{"AggSink"}.withChildren({windowAgg})};
    const LogicalPlan plan(INVALID_QUERY_ID, {sink});
    EXPECT_EQ(flatten(plan).size(), 4); // <-- set breakpoint here, inspect 'plan'
}

TEST_F(LogicalPlanTest, DebugView_InnerJoin)
{
    /// Source("orders") + Source("payments") → Join(id > orderId, TUMBLING 10s) → Sink
    const LogicalOperator sourceOrders{SourceNameLogicalOperator{"orders"}};
    const LogicalOperator sourcePayments{SourceNameLogicalOperator{"payments"}};
    auto joinWindow = std::make_shared<Windowing::TumblingWindow>(
        Windowing::TimeCharacteristic::createIngestionTime(),
        Windowing::TimeMeasure(10000));
    const LogicalOperator join{
        JoinLogicalOperator{
            GreaterLogicalFunction{FieldAccessLogicalFunction{"id"}, FieldAccessLogicalFunction{"orderId"}},
            joinWindow,
            JoinLogicalOperator::JoinType::INNER_JOIN
        }.withChildren({sourceOrders, sourcePayments})};
    const LogicalOperator sink{SinkLogicalOperator{"JoinSink"}.withChildren({join})};
    const LogicalPlan plan(INVALID_QUERY_ID, {sink});
    EXPECT_EQ(flatten(plan).size(), 4); // <-- set breakpoint here, inspect 'plan'
}

/// ── Correctness tests for Debug::view() ──────────────────────────────────────

TEST_F(LogicalPlanTest, DebugView_CleanLabels)
{
    const LogicalOperator source{SourceNameLogicalOperator{"sensors"}};
    const LogicalOperator filter{SelectionLogicalOperator{FieldAccessLogicalFunction{"temperature"}}.withChildren({source})};
    const LogicalOperator sink{SinkLogicalOperator{"ResultSink"}.withChildren({filter})};
    const LogicalPlan plan(INVALID_QUERY_ID, {sink});

    const auto pv = Debug::view(plan);
    ASSERT_EQ(pv.roots.size(), 1);
    EXPECT_EQ(pv.roots[0].op, "SINK(ResultSink)");
    ASSERT_EQ(pv.roots[0].children.size(), 1);
    EXPECT_EQ(pv.roots[0].children[0].op, "SELECTION(temperature)");
    ASSERT_EQ(pv.roots[0].children[0].children.size(), 1);
    EXPECT_EQ(pv.roots[0].children[0].children[0].op, "SOURCE(sensors)");
}

/// view() preserves child order, so a union's two inputs appear in build order.
TEST_F(LogicalPlanTest, DebugView_UnionTwoChildren)
{
    const LogicalOperator sourceA{SourceNameLogicalOperator{"sensorA"}};
    const LogicalOperator sourceB{SourceNameLogicalOperator{"sensorB"}};
    const LogicalOperator unionOp{UnionLogicalOperator{}.withChildren({sourceA, sourceB})};

    const auto ov = Debug::view(unionOp);
    EXPECT_EQ(ov.op, "UnionWith");
    ASSERT_EQ(ov.children.size(), 2);
    EXPECT_EQ(ov.children[0].op, "SOURCE(sensorA)");
    EXPECT_EQ(ov.children[1].op, "SOURCE(sensorB)");
}

TEST_F(LogicalPlanTest, DebugView_JoinLabels)
{
    const LogicalOperator sourceOrders{SourceNameLogicalOperator{"orders"}};
    const LogicalOperator sourcePayments{SourceNameLogicalOperator{"payments"}};
    auto joinWindow = std::make_shared<Windowing::TumblingWindow>(
        Windowing::TimeCharacteristic::createIngestionTime(), Windowing::TimeMeasure(10000));
    const LogicalOperator join{
        JoinLogicalOperator{
            GreaterLogicalFunction{FieldAccessLogicalFunction{"id"}, FieldAccessLogicalFunction{"orderId"}},
            joinWindow,
            JoinLogicalOperator::JoinType::INNER_JOIN}
            .withChildren({sourceOrders, sourcePayments})};
    const LogicalOperator sink{SinkLogicalOperator{"JoinSink"}.withChildren({join})};
    const LogicalPlan plan(INVALID_QUERY_ID, {sink});

    const auto pv = Debug::view(plan);
    ASSERT_EQ(pv.roots.size(), 1);
    EXPECT_EQ(pv.roots[0].op, "SINK(JoinSink)");
    ASSERT_EQ(pv.roots[0].children.size(), 1);
    EXPECT_EQ(pv.roots[0].children[0].op, "Join(id > orderId)");
    ASSERT_EQ(pv.roots[0].children[0].children.size(), 2);
    EXPECT_EQ(pv.roots[0].children[0].children[0].op, "SOURCE(orders)");
    EXPECT_EQ(pv.roots[0].children[0].children[1].op, "SOURCE(payments)");
}

TEST_F(LogicalPlanTest, DebugView_PlanLabelHasQueryId)
{
    const LogicalOperator source{SourceNameLogicalOperator{"sensors"}};
    const LogicalPlan plan(INVALID_QUERY_ID, {source});
    EXPECT_THAT(Debug::view(plan).plan, testing::HasSubstr("queryId"));
}

