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


#include "Phases/DecideFieldMappings.hpp"
#include <functional>


#include <gtest/gtest.h>
#include "Functions/ArithmeticalFunctions/AddLogicalFunction.hpp"
#include "Functions/ConstantValueLogicalFunction.hpp"
#include "Functions/FieldAccessLogicalFunction.hpp"
#include "Operators/ProjectionLogicalOperator.hpp"
#include "Operators/Sinks/SinkLogicalOperator.hpp"
#include "Operators/Sources/SourceDescriptorLogicalOperator.hpp"
#include "Plans/LogicalPlan.hpp"
#include "Sinks/SinkCatalog.hpp"
#include "Sinks/SinkDescriptor.hpp"
#include "Sources/SourceCatalog.hpp"
#include "Traits/FieldMappingTrait.hpp"

namespace NES
{
namespace
{

LogicalSource createLogicalTestSource(SourceCatalog& sourceCatalog)
{
    const UnboundOrderedSchema schema{
        UnboundField{Identifier::parse("attribute_a"), DataType::Type::UINT64},
        UnboundField{Identifier::parse("attribute_b"), DataType::Type::UINT64},
        UnboundField{Identifier::parse("attribute_c"), DataType::Type::VARSIZED}};
    return sourceCatalog.addLogicalSource(Identifier::parse("testSource"), schema).value();
}

SourceDescriptor createTestSourceDescriptor(SourceCatalog& sourceCatalog, const LogicalSource& logicalSource)
{
    const std::unordered_map<std::string, std::string> sourceConfig{{"file_path", "/dev/null"}};
    const std::unordered_map<std::string, std::string> parserConfig{{"type", "CSV"}};
    return sourceCatalog.addPhysicalSource(logicalSource, "file", std::move(sourceConfig), parserConfig).value();
}

SinkDescriptor createTestSinkDescriptor(SinkCatalog& sinkCatalog)
{
    const std::unordered_map<std::string, std::string> sinkConfig{{"file_path", "/dev/null"}, {"input_format", "CSV"}};
    const UnboundOrderedSchema schema{
        UnboundField{Identifier::parse("attribute_a"), DataType::Type::UINT64},
        UnboundField{Identifier::parse("attribute_b"), DataType::Type::UINT64},
        UnboundField{Identifier::parse("attribute_c"), DataType::Type::VARSIZED}};

    return sinkCatalog.addSinkDescriptor(Identifier::parse("testSink"), schema, "file", std::move(sinkConfig)).value();
}

class DecideFieldMappingsTest : public ::testing::Test
{
public:
    explicit DecideFieldMappingsTest()
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

TEST_F(DecideFieldMappingsTest, TestNoCollisions)
{
    SourceDescriptorLogicalOperator sourceDescriptorLogicalOperator{sourceDescriptor};
    const std::vector<std::pair<Identifier, LogicalFunction>> projections{
        {Identifier::parse("attribute_a"),
         AddLogicalFunction{
             FieldAccessLogicalFunction{
                 sourceDescriptorLogicalOperator.getOutputSchema().getFieldByName(Identifier::parse("attribute_a")).value()},
             ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::UINT64), "5"}}},
        {
            Identifier::parse("attribute_b"),
            FieldAccessLogicalFunction{
                sourceDescriptorLogicalOperator.getOutputSchema().getFieldByName(Identifier::parse("attribute_b")).value()},
        },
        {
            Identifier::parse("attribute_c"),
            FieldAccessLogicalFunction{
                sourceDescriptorLogicalOperator.getOutputSchema().getFieldByName(Identifier::parse("attribute_c")).value()},
        }};
    const auto projectionOperator = ProjectionLogicalOperator{projections, ProjectionLogicalOperator::Asterisk{false}}.withChildren(
        {sourceDescriptorLogicalOperator});
    const auto sinkOperator = SinkLogicalOperator{sinkDescriptor}.withChildren({projectionOperator});
    const LogicalPlan logicalPlan{sinkOperator.withInferredSchema()};

    auto annotated = DecideFieldMappings{}.apply(logicalPlan);

    std::function<void(const LogicalOperator&)> recurAssert = [&recurAssert](const LogicalOperator& logicalOperator)
    {
        for (const auto& child : logicalOperator->getChildren())
        {
            recurAssert(child);
        }
        if (logicalOperator.tryGetAs<SinkLogicalOperator>().has_value())
        {
            return;
        }
        const auto outputSchema = logicalOperator.getOutputSchema();
        const auto fieldA = outputSchema.getFieldByName(Identifier::parse("attribute_a")).value();
        const auto fieldB = outputSchema.getFieldByName(Identifier::parse("attribute_b")).value();
        const auto fieldC = outputSchema.getFieldByName(Identifier::parse("attribute_c")).value();
        const auto traitSet = logicalOperator->getTraitSet();
        const auto fieldMappingTrait = traitSet.tryGet<FieldMappingTrait>().value();


        for (const auto& field : {fieldA, fieldB, fieldC})
        {
            const auto fieldHash = std::hash<Field>{}(field);
            const auto fieldInMap = *std::ranges::find_if(
                fieldMappingTrait.getUnderlying() | std::views::keys,
                [&field](const auto& iter) { return iter.getLastName() == field.getLastName(); });
            const auto fieldHashInMap = std::hash<Field>{}(fieldInMap);
            const auto mapping = fieldMappingTrait.getMapping(field);
            EXPECT_TRUE(mapping.has_value());
            EXPECT_EQ(IdentifierList{field.getLastName()}, mapping.value());
        }
    };

    recurAssert(annotated.getRootOperators().at(0));
}

TEST_F(DecideFieldMappingsTest, TestCollisions)
{
    SourceDescriptorLogicalOperator sourceDescriptorLogicalOperator{sourceDescriptor};
    const std::vector<std::pair<Identifier, LogicalFunction>> projections{
        {Identifier::parse("attribute_a"),
         AddLogicalFunction{
             FieldAccessLogicalFunction{
                 sourceDescriptorLogicalOperator.getOutputSchema().getFieldByName(Identifier::parse("attribute_a")).value()},
             ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::UINT64), "5"}}},
        {Identifier::parse("attribute_b"),
         AddLogicalFunction{
             FieldAccessLogicalFunction{
                 sourceDescriptorLogicalOperator.getOutputSchema().getFieldByName(Identifier::parse("attribute_b")).value()},
             FieldAccessLogicalFunction{
                 sourceDescriptorLogicalOperator.getOutputSchema().getFieldByName(Identifier::parse("attribute_a")).value()},
         }},
        {
            Identifier::parse("attribute_c"),
            FieldAccessLogicalFunction{
                sourceDescriptorLogicalOperator.getOutputSchema().getFieldByName(Identifier::parse("attribute_c")).value()},
        }};
    const auto projectionOperator = ProjectionLogicalOperator{projections, ProjectionLogicalOperator::Asterisk{false}}.withChildren(
        {sourceDescriptorLogicalOperator});
    const auto sinkOperator = SinkLogicalOperator{sinkDescriptor}.withChildren({projectionOperator});
    const LogicalPlan logicalPlan{sinkOperator.withInferredSchema()};

    auto annotated = DecideFieldMappings{}.apply(logicalPlan);

    std::function<void(const LogicalOperator&)> recurAssert = [&recurAssert](const LogicalOperator& logicalOperator)
    {
        for (const auto& child : logicalOperator->getChildren())
        {
            recurAssert(child);
        }
        if (logicalOperator.tryGetAs<SinkLogicalOperator>().has_value()
            || logicalOperator.tryGetAs<SourceDescriptorLogicalOperator>().has_value())
        {
            return;
        }
        const auto outputSchema = logicalOperator.getOutputSchema();
        const auto fieldA = outputSchema.getFieldByName(Identifier::parse("attribute_a")).value();
        const auto fieldB = outputSchema.getFieldByName(Identifier::parse("attribute_b")).value();
        const auto fieldC = outputSchema.getFieldByName(Identifier::parse("attribute_c")).value();
        const auto traitSet = logicalOperator->getTraitSet();
        const auto fieldMappingTrait = traitSet.tryGet<FieldMappingTrait>().value();
        const auto mapping = fieldMappingTrait.getMapping(fieldA).value();
        const auto expected = IdentifierList::create(Identifier::parse("attribute_a"), Identifier::parse("new"));
        EXPECT_EQ(expected, mapping);

        for (const auto& field : {fieldB, fieldC})
        {
            EXPECT_EQ(IdentifierList{field.getLastName()}, fieldMappingTrait.getMapping(field).value());
        }
    };
    recurAssert(annotated.getRootOperators().at(0));
}
}

}
