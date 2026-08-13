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
#include <variant>
#include <vector>
#include <Sources/LogicalSource.hpp>

#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>

#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/CalcTargetOrderRule.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>

#include <Configurations/ConfigField.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>
#include <DistributedQuery.hpp>
#include <QueryId.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace
{
LogicalSource createLogicalTestSource(SharedPtr<SourceCatalog>& sourceCatalog)
{
    const Schema<UnqualifiedUnboundField, Ordered> schema{
        UnqualifiedUnboundField{Identifier::parse("attribute_a"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("attribute_b"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("attribute_c"), DataType::Type::VARSIZED}};
    return sourceCatalog->addLogicalSource(Identifier::parse("testSource"), schema).value();
}

SourceDescriptor createTestSourceDescriptor(SharedPtr<SourceCatalog>& sourceCatalog, const LogicalSource& logicalSource)
{
    const Schema<LiteralConfigValue, Ordered> values{
        LiteralConfigValue{QualifiedIdentifier::parse("file_path"), "/dev/null"},
        LiteralConfigValue{QualifiedIdentifier::parse("host"), "localhost"},
        LiteralConfigValue{QualifiedIdentifier::parse("type"), "CSV"}};
    auto configSchema = SourceCatalog::getConfigSchema(Identifier::parse("file"), Identifier::parse("CSV")).value();
    auto [generalConfig, pluginConfig, inputFormatterDescriptor, declaredSchema] = configSchema.resolveConfigs(values).value();
    return sourceCatalog
        ->registerWithLogicalSource(
            PhysicalSourceBuilder{
                std::move(generalConfig), std::move(pluginConfig), std::move(inputFormatterDescriptor), copyPtr(sourceCatalog)},
            logicalSource.getLogicalSourceName())
        .value();
}

SinkDescriptor createTestSinkDescriptor(SinkCatalog& sinkCatalog)
{
    auto [generalConfig, sinkSchema, pluginSinkConfig, outputFormatterDescriptor]
        = SinkCatalog::getConfigSchema(Identifier::parse("file"), Identifier::parse("CSV"))
              .value()
              .resolveConfigs(Schema<LiteralConfigValue, Ordered>{std::vector<LiteralConfigValue>{
                  {QualifiedIdentifier::parse("FILE_SINK.FILE_PATH"), std::string{"/dev/null"}},
                  {QualifiedIdentifier::parse("OUTPUT_FORMATTER.TYPE"), std::string{"CSV"}}}})
              .value();
    generalConfig.host = Host{"localhost"};
    return sinkCatalog.createAnonymousSinkDescriptor(
        std::monostate{}, std::move(generalConfig), std::move(pluginSinkConfig), std::move(outputFormatterDescriptor));
}
}

class CalcTargetOrderTest : public ::testing::Test
{
public:
    explicit CalcTargetOrderTest()
        : logicalSource(createLogicalTestSource(sourceCatalog))
        , sourceDescriptor(createTestSourceDescriptor(sourceCatalog, logicalSource))
        , sinkDescriptor(createTestSinkDescriptor(sinkCatalog))
    {
    }

protected:
    void SetUp() override { }

    SharedPtr<SourceCatalog> sourceCatalog = SourceCatalog::create();
    SinkCatalog sinkCatalog;
    LogicalSource logicalSource;
    SourceDescriptor sourceDescriptor;
    SinkDescriptor sinkDescriptor;
};

TEST_F(CalcTargetOrderTest, JustSource)
{
    const auto sourceOp = SourceDescriptorLogicalOperator::create(sourceDescriptor);
    const auto sinkOp = SinkLogicalOperator::create(sourceOp, sinkDescriptor);

    LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp}};
    plan = CalcTargetOrderRule{}.apply(plan);

    auto targetSchema = std::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(
        plan.getRootOperators()[0].getAs<SinkLogicalOperator>()->getSinkDescriptor()->getSchema());
    EXPECT_EQ(*targetSchema, *logicalSource.getSchema());
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

    LogicalPlan plan{QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()), {sinkOp}};
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
