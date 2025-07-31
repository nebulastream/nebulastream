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


#include <functional>
#include <Phases/DecideFieldMappings.hpp>


#include <gtest/gtest.h>
#include "Functions/ArithmeticalFunctions/AddLogicalFunction.hpp"
#include "Functions/ArithmeticalFunctions/SubLogicalFunction.hpp"
#include "Functions/CastToTypeLogicalFunction.hpp"
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
    const Schema<UnqualifiedUnboundField, Ordered> schema{
        UnqualifiedUnboundField{Identifier::parse("attribute_a"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("attribute_b"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("attribute_c"), DataType::Type::VARSIZED}};

    return sinkCatalog.addSinkDescriptor(Identifier::parse("testSink"), schema, Identifier::parse("file"), std::move(sinkConfig)).value();
}

SinkDescriptor createTestSinkDescriptorWithNewField(SinkCatalog& sinkCatalog)
{
    const std::unordered_map<Identifier, std::string> sinkConfig{
        {Identifier::parse("file_path"), "/dev/null"}, {Identifier::parse("input_format"), "CSV"}};
    const Schema<UnqualifiedUnboundField, Ordered> schema{
        UnqualifiedUnboundField{Identifier::parse("attribute_a"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("attribute_b"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("attribute_c"), DataType::Type::VARSIZED},
        UnqualifiedUnboundField{Identifier::parse("new_field"), DataType::Type::INT64}};

    return sinkCatalog.addSinkDescriptor(Identifier::parse("testSinkWithNew"), schema, Identifier::parse("file"), std::move(sinkConfig))
        .value();
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
    TypedLogicalOperator<SourceDescriptorLogicalOperator> sourceDescriptorLogicalOperator{sourceDescriptor};
    const std::vector<std::pair<Identifier, LogicalFunction>> projections{
        {Identifier::parse("attribute_a"),
         AddLogicalFunction{
             FieldAccessLogicalFunction{
                 sourceDescriptorLogicalOperator->getOutputSchema().getFieldByName(Identifier::parse("attribute_a")).value()},
             ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::UINT64), "5"}}},
        {
            Identifier::parse("attribute_b"),
            FieldAccessLogicalFunction{
                sourceDescriptorLogicalOperator->getOutputSchema().getFieldByName(Identifier::parse("attribute_b")).value()},
        },
        {
            Identifier::parse("attribute_c"),
            FieldAccessLogicalFunction{
                sourceDescriptorLogicalOperator->getOutputSchema().getFieldByName(Identifier::parse("attribute_c")).value()},
        }};
    const auto projectionOperator
        = TypedLogicalOperator<ProjectionLogicalOperator>{projections, ProjectionLogicalOperator::Asterisk{false}}.withChildren(
            {sourceDescriptorLogicalOperator});
    const auto sinkOperator = TypedLogicalOperator<SinkLogicalOperator>{sinkDescriptor}.withChildren({projectionOperator});
    const LogicalPlan logicalPlan{sinkOperator->withInferredSchema()};

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
                [&field](const auto& iter) { return iter.getFullyQualifiedName() == field.getFullyQualifiedName(); });
            const auto fieldHashInMap = std::hash<UnqualifiedUnboundField>{}(fieldInMap);
            const auto mapping = fieldMappingTrait.getMapping(field.unbound());
            EXPECT_TRUE(mapping.has_value());
            EXPECT_EQ(IdentifierList{field.getLastName()}, mapping.value());
        }
    };

    recurAssert(annotated.getRootOperators().at(0));
}

TEST_F(DecideFieldMappingsTest, TestCollisions)
{
    TypedLogicalOperator<SourceDescriptorLogicalOperator> sourceDescriptorLogicalOperator{sourceDescriptor};

    /// Equivalent to SELECT attribute_a + 5 as attribute_a, attribute_a + attribute_b as attribute_b, attribute_c as attribute_c
    /// We'd expect attribute_a to receive a redirection mapping while attribute_b/c do not since they are only accessed in one projection
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
    const auto projectionOperator
        = TypedLogicalOperator<ProjectionLogicalOperator>{projections, ProjectionLogicalOperator::Asterisk{false}}.withChildren(
            {sourceDescriptorLogicalOperator});
    const auto sinkOperator = TypedLogicalOperator<SinkLogicalOperator>{sinkDescriptor}.withChildren({projectionOperator});
    const LogicalPlan logicalPlan{sinkOperator->withInferredSchema()};

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
        const auto mapping = fieldMappingTrait.getMapping(fieldA.unbound()).value();
        const auto expected = IdentifierList::create(Identifier::parse("attribute_a"), Identifier::parse("new"));
        EXPECT_EQ(expected, mapping);

        for (const auto& field : {fieldB, fieldC})
        {
            EXPECT_EQ(IdentifierList{field.getLastName()}, fieldMappingTrait.getMapping(field.unbound()).value());
        }
    };
    recurAssert(annotated.getRootOperators().at(0));
}

TEST_F(DecideFieldMappingsTest, TestProjectionToNewField)
{
    TypedLogicalOperator<SourceDescriptorLogicalOperator> sourceDescriptorLogicalOperator{sourceDescriptor};
    const auto sinkDescriptorWithNew = createTestSinkDescriptorWithNewField(sinkCatalog);

    /// Equivalent to SELECT attribute_a, attribute_b, attribute_c, attribute_a - attribute_b AS new_field
    /// This mirrors the failing query: SELECT id, value, timestamp, id - value AS new FROM stream INTO streamSinkWithNew
    /// None of the fields should receive a redirection mapping since there are no read-write conflicts:
    /// - attribute_a, attribute_b, attribute_c are simple pass-throughs
    /// - new_field is a new computed field that doesn't overwrite any existing field
    const std::vector<std::pair<Identifier, LogicalFunction>> projections{
        {
            Identifier::parse("attribute_a"),
            FieldAccessLogicalFunction{
                sourceDescriptorLogicalOperator->getOutputSchema().getFieldByName(Identifier::parse("attribute_a")).value()},
        },
        {
            Identifier::parse("attribute_b"),
            FieldAccessLogicalFunction{
                sourceDescriptorLogicalOperator->getOutputSchema().getFieldByName(Identifier::parse("attribute_b")).value()},
        },
        {
            Identifier::parse("attribute_c"),
            FieldAccessLogicalFunction{
                sourceDescriptorLogicalOperator->getOutputSchema().getFieldByName(Identifier::parse("attribute_c")).value()},
        },
        {Identifier::parse("new_field"),
         SubLogicalFunction{
             CastToTypeLogicalFunction{
                 DataTypeProvider::provideDataType(DataType::Type::INT64),
                 {FieldAccessLogicalFunction{
                     sourceDescriptorLogicalOperator->getOutputSchema().getFieldByName(Identifier::parse("attribute_a")).value()}}},
             CastToTypeLogicalFunction{
                 DataTypeProvider::provideDataType(DataType::Type::INT64),
                 FieldAccessLogicalFunction{
                     sourceDescriptorLogicalOperator->getOutputSchema().getFieldByName(Identifier::parse("attribute_b")).value()}}}},
    };
    const auto projectionOperator
        = TypedLogicalOperator<ProjectionLogicalOperator>{projections, ProjectionLogicalOperator::Asterisk{false}}.withChildren(
            {sourceDescriptorLogicalOperator});
    const auto sinkOperator = TypedLogicalOperator<SinkLogicalOperator>{sinkDescriptorWithNew}.withChildren({projectionOperator});
    const LogicalPlan logicalPlan{sinkOperator->withInferredSchema()};

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
        const auto newField = outputSchema.getFieldByName(Identifier::parse("new_field")).value();
        const auto traitSet = logicalOperator->getTraitSet();
        const auto fieldMappingTrait = traitSet.tryGet<FieldMappingTrait>().value();

        /// All fields should have simple mappings (no redirects) since there are no read-write conflicts
        for (const auto& field : {fieldA, fieldB, fieldC, newField})
        {
            const auto mapping = fieldMappingTrait.getMapping(field.unbound());
            EXPECT_TRUE(mapping.has_value());
            EXPECT_EQ(IdentifierList{field.getLastName()}, mapping.value());
        }
    };
    recurAssert(annotated.getRootOperators().at(0));
}
}

}
