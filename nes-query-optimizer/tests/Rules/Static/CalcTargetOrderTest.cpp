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
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Sources/LogicalSource.hpp>

#include <Operators/Sinks/SinkLogicalOperator.hpp>

#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/CalcTargetOrderRule.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>

#include <Identifiers/Identifiers.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace
{
LogicalSource createLogicalTestSource() {
    const Schema<UnqualifiedUnboundField, Ordered> schema{
        UnqualifiedUnboundField{Identifier::parse("attribute_a"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("attribute_b"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("attribute_c"), DataType::Type::VARSIZED}};
    return LogicalSource{Identifier::parse("testSource"), schema};
}

SourceDescriptor createTestSourceDescriptor(const LogicalSource &logicalSource) {
    const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("file_path"), "/dev/null"}};
    return SourceDescriptor::create(
                PhysicalSourceId{1},
                logicalSource,
                Identifier::parse("File"),
                Host{"localhost"},
                sourceConfig,
                {{Identifier::parse("type"), "CSV"}})
            .value();
}

SinkDescriptor createTestSinkDescriptor() {
    const std::unordered_map<Identifier, std::string> sinkConfig{
        {Identifier::parse("file_path"), "/dev/null"}, {Identifier::parse("output_format"), "CSV"}};

    return SinkDescriptor::createInline(INITIAL_SINK_ID, std::nullopt, Identifier::parse("file"), Host{"localhost"},
                                        sinkConfig, {})
            .value();
}
}

class CalcTargetOrderTest : public ::testing::Test
{
public:
    explicit CalcTargetOrderTest()
        : logicalSource(createLogicalTestSource())
          , sourceDescriptor(createTestSourceDescriptor(logicalSource))
          , sinkDescriptor(createTestSinkDescriptor()) {
    }

protected:
    void SetUp() override { }

    LogicalSource logicalSource;
    SourceDescriptor sourceDescriptor;
    SinkDescriptor sinkDescriptor;
};

TEST_F(CalcTargetOrderTest, JustSource)
{
    const auto sourceOp = SourceDescriptorLogicalOperator::create(sourceDescriptor);
    const auto sinkOp = SinkLogicalOperator::create(sourceOp, sinkDescriptor);

    LogicalPlan plan{QueryId{1}, {sinkOp}};
    plan = CalcTargetOrderRule{}.apply(plan);

    auto targetSchema = std::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(
        plan.getRootOperators()[0].getAs<SinkLogicalOperator>()->getSinkDescriptor()->getSchema());
    EXPECT_EQ(*targetSchema, *sourceDescriptor.getLogicalSource().getSchema());
}

TEST_F(CalcTargetOrderTest, JoinOverProjection)
{
    const auto sourceOp1 = SourceDescriptorLogicalOperator::create(sourceDescriptor);
    const auto sourceOp2 = SourceDescriptorLogicalOperator::create(sourceDescriptor);
    const std::vector<std::pair<Identifier, LogicalFunction>> projections{
        {Identifier::parse("projection_b"),
         FieldAccessLogicalFunction{sourceOp2->getOutputSchema()[Identifier::parse("attribute_b")].value()}},
        {Identifier::parse("projection_a"),
         FieldAccessLogicalFunction{sourceOp2->getOutputSchema()[Identifier::parse("attribute_a")].value()}},
    };
    auto projectionOp = ProjectionLogicalOperator::create(sourceOp2, projections, ProjectionLogicalOperator::Asterisk{false});
    auto joinPredicate = EqualsLogicalFunction{
        FieldAccessLogicalFunction{sourceOp1->getOutputSchema()[Identifier::parse("attribute_a")].value()},
        FieldAccessLogicalFunction{projectionOp->getOutputSchema()[Identifier::parse("projection_a")].value()}};
    constexpr uint64_t windowSizeMs = 1000;
    auto joinOp = JoinLogicalOperator::create(
        {sourceOp1, projectionOp},
        joinPredicate,
        Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{windowSizeMs}}},
        JoinLogicalOperator::JoinType::INNER_JOIN,
        JoinTimeCharacteristic{std::array{
            Windowing::BoundTimeCharacteristic{Windowing::TimeCharacteristicWrapper::createIngestionTime()},
            Windowing::BoundTimeCharacteristic{Windowing::TimeCharacteristicWrapper::createIngestionTime()}}});
    const auto sinkOp = SinkLogicalOperator::create(joinOp, sinkDescriptor);

    LogicalPlan plan{QueryId{1}, {sinkOp}};
    plan = CalcTargetOrderRule{}.apply(plan);

    const Schema<UnqualifiedUnboundField, Ordered> expectedSchema{
        UnqualifiedUnboundField{Identifier::parse("attribute_a"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("attribute_b"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("attribute_c"), DataType::Type::VARSIZED},
        UnqualifiedUnboundField{Identifier::parse("projection_b"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("projection_a"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("end"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("start"), DataType::Type::UINT64}};

    const auto newTargetSchema = std::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(
        plan.getRootOperators()[0].getAs<SinkLogicalOperator>()->getSinkDescriptor()->getSchema());
    EXPECT_EQ(*newTargetSchema, expectedSchema);
}
}

/// NOLINTEND(bugprone-unchecked-optional-access)
