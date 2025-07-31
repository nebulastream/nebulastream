#include "Sources/LogicalSource.hpp"

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

#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <gtest/gtest.h>
#include "Functions/BooleanFunctions/AndLogicalFunction.hpp"
#include "Functions/BooleanFunctions/EqualsLogicalFunction.hpp"
#include "Functions/FieldAccessLogicalFunction.hpp"
#include "LegacyOptimizer/CalcTargetOrderPhase.hpp"
#include "Operators/LogicalOperator.hpp"
#include "Operators/ProjectionLogicalOperator.hpp"
#include "Operators/Sources/SourceDescriptorLogicalOperator.hpp"
#include "Operators/Windows/JoinLogicalOperator.hpp"
#include "Plans/LogicalPlan.hpp"
#include "Traits/FieldOrderingTrait.hpp"
#include "WindowTypes/Types/TumblingWindow.hpp"

namespace NES
{
namespace
{
LogicalSource createLogicalTestSource(SourceCatalog& sourceCatalog)
{
    const Schema<UnqualifiedUnboundField, Ordered> schema{
        UnqualifiedUnboundField{Identifier::parse("attribute_a"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("attribute_b"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("attribute_c"), DataType::Type::VARSIZED}};
    return sourceCatalog.addLogicalSource(Identifier::parse("testSource"), schema).value();
}

SourceDescriptor createTestSourceDescriptor(SourceCatalog& sourceCatalog, const LogicalSource& logicalSource)
{
    const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("file_path"), "/dev/null"}};
    const std::unordered_map<Identifier, std::string> parserConfig{{Identifier::parse("type"), "CSV"}};
    return sourceCatalog.addPhysicalSource(logicalSource, Identifier::parse("file"), std::move(sourceConfig), parserConfig).value();
}

SinkDescriptor createTestSinkDescriptor(SinkCatalog& sinkCatalog)
{
    const std::unordered_map<Identifier, std::string> sinkConfig{
        {Identifier::parse("file_path"), "/dev/null"}, {Identifier::parse("input_format"), "CSV"}};

    return sinkCatalog.getInlineSink(std::nullopt, Identifier::parse("file"), std::move(sinkConfig)).value();
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

    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
    LogicalSource logicalSource;
    SourceDescriptor sourceDescriptor;
    SinkDescriptor sinkDescriptor;
};

TEST_F(CalcTargetOrderTest, JustSource)
{
    TypedLogicalOperator<SourceDescriptorLogicalOperator> sourceOp{sourceDescriptor};
    const auto sinkOp = TypedLogicalOperator<SinkLogicalOperator>
    {
        sinkDescriptor
    } -> withChildren({sourceOp});

    auto plan = LogicalPlan{sinkOp.withInferredSchema()};
    CalcTargetOrderPhase{}.apply(plan);

    auto targetSchema = std::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(
        plan.getRootOperators()[0].getAs<SinkLogicalOperator>()->getSinkDescriptor()->getSchema());
    EXPECT_EQ(*targetSchema, *sourceDescriptor.getLogicalSource().getSchema());
}

TEST_F(CalcTargetOrderTest, JoinOverProjection)
{
    TypedLogicalOperator<SourceDescriptorLogicalOperator> sourceOp1{sourceDescriptor};
    TypedLogicalOperator<SourceDescriptorLogicalOperator> sourceOp2{sourceDescriptor};
    std::vector<std::pair<Identifier, LogicalFunction>> projections{
        {Identifier::parse("projection_b"),
         FieldAccessLogicalFunction{sourceOp2->getOutputSchema()[Identifier::parse("attribute_b")].value()}},
        {Identifier::parse("projection_a"),
         FieldAccessLogicalFunction{sourceOp2->getOutputSchema()[Identifier::parse("attribute_a")].value()}},
    };
    auto projectionOp = TypedLogicalOperator<ProjectionLogicalOperator>{projections, ProjectionLogicalOperator::Asterisk{false}}
                            .withChildren({sourceOp2})
                            .withInferredSchema();
    auto joinPredicate = EqualsLogicalFunction{
        FieldAccessLogicalFunction{sourceOp1->getOutputSchema()[Identifier::parse("attribute_a")].value()},
        FieldAccessLogicalFunction{projectionOp->getOutputSchema()[Identifier::parse("projection_a")].value()}};
    auto joinOp = TypedLogicalOperator<JoinLogicalOperator>{
        joinPredicate,
        std::make_shared<Windowing::TumblingWindow>(Windowing::TimeMeasure{1000}),
        JoinLogicalOperator::JoinType::INNER_JOIN,
        JoinTimeCharacteristic{std::array{
            Windowing::BoundTimeCharacteristic{Windowing::TimeCharacteristicWrapper::createIngestionTime()},
            Windowing::BoundTimeCharacteristic{Windowing::TimeCharacteristicWrapper::createIngestionTime()}}}};
    joinOp = joinOp.withChildren({sourceOp1, projectionOp});
    const auto sinkOp = TypedLogicalOperator<SinkLogicalOperator>
    {
        sinkDescriptor
    } -> withChildren({joinOp}).withInferredSchema();

    auto plan = LogicalPlan{sinkOp};
    CalcTargetOrderPhase{}.apply(plan);

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

// TEST_F(CalcTargetOrderTest, JustSource)
// {
//     TypedLogicalOperator<SourceDescriptorLogicalOperator> sourceOp{sourceDescriptor};
//     const auto sinkOp = TypedLogicalOperator<SinkLogicalOperator>{sinkDescriptor}->withChildren({sourceOp});
//
//     auto plan = LogicalPlan{sinkOp.withInferredSchema()};
//     CalcTargetOrderPhase{}.apply(plan);
//
//     auto targetSchema = std::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(
//         plan.getRootOperators()[0].getAs<SinkLogicalOperator>()->getSinkDescriptor()->getSchema());
//     EXPECT_EQ(*targetSchema, *sourceDescriptor.getLogicalSource().getSchema());
// }
//
// TEST_F(CalcTargetOrderTest, SimpleOrderingTrait)
// {
//     TypedLogicalOperator<SourceDescriptorLogicalOperator> sourceOp{sourceDescriptor};
//     const auto sinkOp = TypedLogicalOperator<SinkLogicalOperator>{sinkDescriptor}->withChildren({sourceOp});
//
//     auto plan = LogicalPlan{sinkOp};
//     CalcTargetOrderPhase{}.apply(plan);
//
//     const auto newSource = plan.getRootOperators().at(0)->getChildren()[0];
//
//     const auto sourceFieldOrdering = newSource.getTraitSet().get<FieldOrderingTrait>();
//
//     const auto expectedSchema = std::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(sinkDescriptor.getSchema());
//     EXPECT_EQ(*expectedSchema, sourceFieldOrdering.getOrderedFields());
// }
//
// TEST_F(CalcTargetOrderTest, UnionOrdering)
// {
//     TypedLogicalOperator<SourceDescriptorLogicalOperator> sourceOp{sourceDescriptor};
//     const auto sinkOp = TypedLogicalOperator<SinkLogicalOperator>{sinkDescriptor}->withChildren({sourceOp});
// }

}
