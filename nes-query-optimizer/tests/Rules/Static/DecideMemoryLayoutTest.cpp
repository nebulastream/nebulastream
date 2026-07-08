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
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Static/DecideMemoryLayoutRule.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/UUID.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>

#include <DistributedQuery.hpp>
#include <QueryId.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace
{
constexpr uint64_t TUMBLING_WINDOW_SIZE_MS = 1000;

LogicalSource createLogicalTestSource(SourceCatalog& sourceCatalog, const std::string& name)
{
    const Schema<UnqualifiedUnboundField, Ordered> schema{
        UnqualifiedUnboundField{Identifier::parse(name + "_id"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse(name + "_value"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse(name + "_ts"), DataType::Type::UINT64}};
    return sourceCatalog.addLogicalSource(Identifier::parse(name), schema).value();
}

SourceDescriptor createTestSourceDescriptor(SourceCatalog& sourceCatalog, const LogicalSource& logicalSource)
{
    const Schema<LiteralConfigValue, Ordered> sourceConfig{{"file_path", "/dev/null"}, {"host", "localhost"}};
    const Schema<LiteralConfigValue, Ordered> parserConfig{{"type", "CSV"}};
    return sourceCatalog.addPhysicalSource(logicalSource, Identifier::parse("file"), sourceConfig, parserConfig).value();
}

SinkDescriptor createTestSinkDescriptor(SinkCatalog& sinkCatalog)
{
    const std::unordered_map<Identifier, std::string> sinkConfig{
        {Identifier::parse("file_path"), "/dev/null"}, {Identifier::parse("output_format"), "CSV"}};
    return sinkCatalog.getInlineSink(std::nullopt, Identifier::parse("file"), Host("localhost"), sinkConfig, {}).value();
}
}

class DecideMemoryLayoutTest : public ::testing::Test
{
public:
    explicit DecideMemoryLayoutTest()
        : leftSource(createLogicalTestSource(sourceCatalog, "left"))
        , rightSource(createLogicalTestSource(sourceCatalog, "right"))
        , leftSourceDescriptor(createTestSourceDescriptor(sourceCatalog, leftSource))
        , rightSourceDescriptor(createTestSourceDescriptor(sourceCatalog, rightSource))
        , sinkDescriptor(createTestSinkDescriptor(sinkCatalog))
    {
    }

protected:
    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
    LogicalSource leftSource;
    LogicalSource rightSource;
    SourceDescriptor leftSourceDescriptor;
    SourceDescriptor rightSourceDescriptor;
    SinkDescriptor sinkDescriptor;
};

/// Source → Sink. Verify all operators get ROW_LAYOUT.
TEST_F(DecideMemoryLayoutTest, SingleOperatorGetsRowLayout)
{
    const auto sourceOp = SourceDescriptorLogicalOperator::create(leftSourceDescriptor);
    const auto sinkOp = SinkLogicalOperator::create(sourceOp, sinkDescriptor);
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp}};

    const auto result = DecideMemoryLayoutRule{}.apply(plan);

    for (const auto& op : BFSRange(result.getRootOperators()[0]))
    {
        ASSERT_TRUE(op.getTraitSet().contains<MemoryLayoutTypeTrait>());
        const auto trait = op.getTraitSet().get<MemoryLayoutTypeTrait>();
        EXPECT_TRUE(trait->memoryLayout == MemoryLayoutType::ROW_LAYOUT);
    }
}

/// Source → Join ← Source → Sink. Verify all operators get the trait.
TEST_F(DecideMemoryLayoutTest, BinaryPlanAllGetRowLayout)
{
    const auto leftSourceOp = SourceDescriptorLogicalOperator::create(leftSourceDescriptor);
    const auto rightSourceOp = SourceDescriptorLogicalOperator::create(rightSourceDescriptor);

    auto joinFunction = LogicalFunction{EqualsLogicalFunction{
        FieldAccessLogicalFunction{leftSourceOp->getOutputSchema()[Identifier::parse("left_id")].value()},
        FieldAccessLogicalFunction{rightSourceOp->getOutputSchema()[Identifier::parse("right_id")].value()}}};

    const auto joinOp = JoinLogicalOperator::create(
        {leftSourceOp, rightSourceOp},
        std::move(joinFunction),
        Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{TUMBLING_WINDOW_SIZE_MS}}},
        JoinLogicalOperator::JoinType::INNER_JOIN,
        JoinTimeCharacteristic{std::array{
            Windowing::BoundTimeCharacteristic{Windowing::TimeCharacteristicWrapper::createIngestionTime()},
            Windowing::BoundTimeCharacteristic{Windowing::TimeCharacteristicWrapper::createIngestionTime()}}});
    const auto sinkOp = SinkLogicalOperator::create(joinOp, sinkDescriptor);
    const LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp}};

    const auto result = DecideMemoryLayoutRule{}.apply(plan);

    for (const auto& op : BFSRange(result.getRootOperators()[0]))
    {
        ASSERT_TRUE(op.getTraitSet().contains<MemoryLayoutTypeTrait>()) << "Operator missing MemoryLayoutTypeTrait";
        const auto trait = op.getTraitSet().get<MemoryLayoutTypeTrait>();
        EXPECT_TRUE(trait->memoryLayout == MemoryLayoutType::ROW_LAYOUT);
    }
}
}

/// NOLINTEND(bugprone-unchecked-optional-access)
