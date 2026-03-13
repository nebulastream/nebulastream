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

#include <cstdint>
#include <memory>
#include <vector>

#include <BaseUnitTest.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>

#include <QueryOptimizer.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <Traits/ImplementationTypeTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace NES
{
namespace
{

class QueryOptimizerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("QueryOptimizerTest.log", LogLevel::LOG_DEBUG); }

    static constexpr uint64_t TUMBLING_WINDOW_SIZE_MS = 1000;

    static LogicalPlan createSourcePlan(const std::string& sourceType, const Schema& schema)
    {
        return LogicalPlanBuilder::createLogicalPlan(sourceType, schema, {}, {});
    }

    static Schema createSchema(const std::string& prefix)
    {
        Schema schema;
        schema.addField(prefix + ".id", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        schema.addField(prefix + ".value", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        schema.addField(prefix + ".ts", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        return schema;
    }

    static std::shared_ptr<Windowing::WindowType> createTumblingWindow()
    {
        return std::make_shared<Windowing::TumblingWindow>(
            Windowing::TimeCharacteristic::createIngestionTime(), Windowing::TimeMeasure(TUMBLING_WINDOW_SIZE_MS));
    }
};

/// After optimization, every operator in a simple Source-Sink plan should have both JoinImplementation and MemoryLayout traits.
TEST_F(QueryOptimizerTest, SimplePlanHasBothTraitsAfterOptimization)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan("TEST", schema);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    QueryOptimizerConfiguration config;
    config.joinStrategy = StreamJoinStrategy::OPTIMIZER_CHOOSES;
    auto optimized = QueryOptimizer::optimize(plan, config);

    for (const auto& op : BFSRange(optimized.getPlan().getRootOperators()[0]))
    {
        EXPECT_TRUE(op.getTraitSet().contains<JoinImplementationTypeTrait>())
            << "Operator should have JoinImplementationTypeTrait after optimization";
        EXPECT_TRUE(op.getTraitSet().contains<MemoryLayoutTypeTrait>())
            << "Operator should have MemoryLayoutTypeTrait after optimization";
    }
}

/// Non-join operators should get CHOICELESS join trait and ROW_LAYOUT memory trait.
TEST_F(QueryOptimizerTest, NonJoinOperatorsGetChoicelessAndRowLayout)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan("TEST", schema);
    auto selectionFn = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("src.id")}, LogicalFunction{FieldAccessLogicalFunction("src.id")})};
    plan = LogicalPlanBuilder::addSelection(selectionFn, plan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    QueryOptimizerConfiguration config;
    config.joinStrategy = StreamJoinStrategy::OPTIMIZER_CHOOSES;
    auto optimized = QueryOptimizer::optimize(plan, config);

    for (const auto& op : BFSRange(optimized.getPlan().getRootOperators()[0]))
    {
        auto joinTrait = op.getTraitSet().get<JoinImplementationTypeTrait>();
        EXPECT_EQ(joinTrait->implementationType, JoinImplementation::CHOICELESS);

        auto memTrait = op.getTraitSet().get<MemoryLayoutTypeTrait>();
        EXPECT_EQ(memTrait->memoryLayout, MemoryLayoutType::ROW_LAYOUT);
    }
}

/// A join plan optimized with OPTIMIZER_CHOOSES and an equality condition should get HASH_JOIN + ROW_LAYOUT.
TEST_F(QueryOptimizerTest, JoinPlanGetsHashJoinAndRowLayout)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    auto joinFunction = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{FieldAccessLogicalFunction("left.id")}, LogicalFunction{FieldAccessLogicalFunction("right.id")})};

    auto plan
        = LogicalPlanBuilder::addJoin(leftPlan, rightPlan, joinFunction, createTumblingWindow(), JoinLogicalOperator::JoinType::INNER_JOIN);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    QueryOptimizerConfiguration config;
    config.joinStrategy = StreamJoinStrategy::OPTIMIZER_CHOOSES;
    auto optimized = QueryOptimizer::optimize(plan, config);

    auto joins = getOperatorByType<JoinLogicalOperator>(optimized.getPlan());
    ASSERT_EQ(joins.size(), 1);

    auto joinTrait = joins[0]->getTraitSet().get<JoinImplementationTypeTrait>();
    EXPECT_EQ(joinTrait->implementationType, JoinImplementation::HASH_JOIN);

    auto memTrait = joins[0]->getTraitSet().get<MemoryLayoutTypeTrait>();
    EXPECT_EQ(memTrait->memoryLayout, MemoryLayoutType::ROW_LAYOUT);
}

/// Optimizer preserves the query ID through the optimization pipeline.
TEST_F(QueryOptimizerTest, QueryIdIsPreserved)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan("TEST", schema);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    const auto originalQueryId = plan.getQueryId();

    QueryOptimizerConfiguration config;
    auto optimized = QueryOptimizer::optimize(plan, config);

    EXPECT_EQ(optimized.getPlan().getQueryId(), originalQueryId);
}

/// The instance-based optimize method produces the same result as the static one.
TEST_F(QueryOptimizerTest, InstanceMethodMatchesStaticMethod)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan("TEST", schema);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    QueryOptimizerConfiguration config;
    config.joinStrategy = StreamJoinStrategy::NESTED_LOOP_JOIN;

    auto staticResult = QueryOptimizer::optimize(plan, config);
    QueryOptimizer optimizer(config);
    auto instanceResult = optimizer.optimize(plan);

    /// Both should produce plans with the same root operator count.
    EXPECT_EQ(staticResult.getPlan().getRootOperators().size(), instanceResult.getPlan().getRootOperators().size());
    EXPECT_EQ(staticResult.getPlan().getQueryId(), instanceResult.getPlan().getQueryId());
}

/// Union plan: both branches and the union operator get traits after optimization.
TEST_F(QueryOptimizerTest, UnionPlanAllOperatorsGetTraits)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan("TEST", leftSchema);
    auto rightPlan = createSourcePlan("TEST", rightSchema);

    auto plan = LogicalPlanBuilder::addUnion(leftPlan, rightPlan);
    plan = LogicalPlanBuilder::addSink("test_sink", plan);

    QueryOptimizerConfiguration config;
    config.joinStrategy = StreamJoinStrategy::OPTIMIZER_CHOOSES;
    auto optimized = QueryOptimizer::optimize(plan, config);

    for (const auto& op : BFSRange(optimized.getPlan().getRootOperators()[0]))
    {
        EXPECT_TRUE(op.getTraitSet().contains<JoinImplementationTypeTrait>());
        EXPECT_TRUE(op.getTraitSet().contains<MemoryLayoutTypeTrait>());
    }
}

}
}
