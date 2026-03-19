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

#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <Phases/DecideMemoryLayout.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>

namespace NES
{
namespace
{

class DecideMemoryLayoutTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("DecideMemoryLayoutTest.log", LogLevel::LOG_DEBUG); }

    static constexpr uint64_t TUMBLING_WINDOW_SIZE_MS = 1000;

    /// Helper to create a simple source plan with a schema containing an "id" field
    static LogicalPlan createSourcePlan(const Identifier& sourceType, const Schema<UnqualifiedUnboundField, Ordered>& schema)
    {
        return LogicalPlanBuilder::createLogicalPlan(sourceType, schema, {}, {});
    }

    static Schema<UnqualifiedUnboundField, Ordered> createSchema(const std::string& prefix)
    {
        return Schema<UnqualifiedUnboundField, Ordered>{
            {Identifier::parse(prefix + "_id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
            {Identifier::parse(prefix + "_value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
            {Identifier::parse(prefix + "_ts"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};
    }

    static Windowing::TimeBasedWindowType createTumblingWindow()
    {
        return Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure(TUMBLING_WINDOW_SIZE_MS)}};
    }
};

/// InlineSource with sink. Verify all get ROW_LAYOUT.
TEST_F(DecideMemoryLayoutTest, SingleOperatorGetsRowLayout)
{
    auto schema = createSchema("src");
    auto plan = createSourcePlan(Identifier::parse("TEST"), schema);
    plan = LogicalPlanBuilder::addSink(Identifier::parse("test_sink"), plan);

    DecideMemoryLayout phase;
    auto result = phase.apply(plan);

    for (const auto& op : BFSRange(result.getRootOperators()[0]))
    {
        ASSERT_TRUE(op.getTraitSet().contains<MemoryLayoutTypeTrait>());
        auto trait = op.getTraitSet().get<MemoryLayoutTypeTrait>();
        EXPECT_TRUE(trait->memoryLayout == MemoryLayoutType::ROW_LAYOUT);
    }
}

/// Source → Join ← Source → Sink. Verify all operators get the trait.
TEST_F(DecideMemoryLayoutTest, BinaryPlanAllGetRowLayout)
{
    auto leftSchema = createSchema("left");
    auto rightSchema = createSchema("right");
    auto leftPlan = createSourcePlan(Identifier::parse("TEST"), leftSchema);
    auto rightPlan = createSourcePlan(Identifier::parse("TEST"), rightSchema);

    auto joinFunction = LogicalFunction{EqualsLogicalFunction(
        LogicalFunction{UnboundFieldAccessLogicalFunction(Identifier::parse("left_id"))},
        LogicalFunction{UnboundFieldAccessLogicalFunction(Identifier::parse("right_id"))})};

    auto plan = LogicalPlanBuilder::addJoin(
        leftPlan,
        rightPlan,
        joinFunction,
        createTumblingWindow(),
        JoinLogicalOperator::JoinType::INNER_JOIN,
        Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}},
        Windowing::BoundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}});
    plan = LogicalPlanBuilder::addSink(Identifier::parse("test_sink"), plan);

    DecideMemoryLayout phase;
    auto result = phase.apply(plan);

    for (const auto& op : BFSRange(result.getRootOperators()[0]))
    {
        ASSERT_TRUE(op.getTraitSet().contains<MemoryLayoutTypeTrait>()) << "Operator missing MemoryLayoutTypeTrait";
        auto trait = op.getTraitSet().get<MemoryLayoutTypeTrait>();
        EXPECT_TRUE(trait->memoryLayout == MemoryLayoutType::ROW_LAYOUT);
    }
}

}
}
