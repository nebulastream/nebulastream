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
#include <functional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SemanticFilterLogicalOperator.hpp>
#include <Operators/SemanticFilterNameLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

/// NOLINTBEGIN(readability-magic-numbers, bugprone-unchecked-optional-access)

namespace
{

UnqualifiedUnboundField
field(const std::string_view name, const DataType::Type type, const DataType::NULLABLE nullable = DataType::NULLABLE::NOT_NULLABLE)
{
    return UnqualifiedUnboundField{Identifier::parse(std::string{name}), DataType{type, nullable}};
}

SemanticModelConfig mockConfig()
{
    SemanticModelConfig config;
    config.backend = "mock";
    config.endpoint = "label:true";
    config.modelName = "mock-model";
    config.fusion = true;
    return config;
}

/// A filter model reading `description`: no OUTPUT clause, one FILTER step.
RegisteredSemanticModel filterModel(SemanticModelCatalog& catalog, const std::string& name)
{
    auto config = mockConfig();
    config.steps = {SemanticStep{
        .kind = SemanticStep::Kind::FILTER, .prompt = "is positive", .outputColumn = {}, .outputValues = {}, .defaultValue = {}}};
    catalog.registerModel(
        name,
        std::move(config),
        SemanticModelSchema{.inputs = SemanticFieldList{field("description", DataType::Type::VARSIZED)}, .outputs = {}});
    return catalog.load(name);
}

/// A map model reading `description` and writing `output`.
RegisteredSemanticModel mapModel(SemanticModelCatalog& catalog, const std::string& name, const std::string& output)
{
    auto config = mockConfig();
    config.steps = {SemanticStep{
        .kind = SemanticStep::Kind::MAP, .prompt = "classify", .outputColumn = output, .outputValues = {}, .defaultValue = {}}};
    catalog.registerModel(
        name,
        std::move(config),
        SemanticModelSchema{
            .inputs = SemanticFieldList{field("description", DataType::Type::VARSIZED)},
            .outputs = SemanticFieldList{field(output, DataType::Type::VARSIZED)}});
    return catalog.load(name);
}

class SemanticFilterLogicalOperatorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SemanticFilterLogicalOperatorTest.log", LogLevel::LOG_DEBUG); }

    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
    SemanticModelCatalog semanticModels;

    TypedLogicalOperator<SourceDescriptorLogicalOperator> source(const std::string_view name, std::vector<UnqualifiedUnboundField> fields)
    {
        const auto logical
            = sourceCatalog
                  .addLogicalSource(
                      Identifier::parse(std::string{name}), fields | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>())
                  .value();
        const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("file_path"), "/dev/null"}};
        const std::unordered_map<Identifier, std::string> parserConfig{{Identifier::parse("type"), "CSV"}};
        const auto descriptor
            = sourceCatalog.addPhysicalSource(logical, Identifier::parse("file"), Host("localhost"), sourceConfig, parserConfig).value();
        return SourceDescriptorLogicalOperator::create(descriptor);
    }

    /// A file sink over `fields`, linked to `child` without inference.
    TypedLogicalOperator<SinkLogicalOperator>
    sinkAbove(const LogicalOperator& child, std::vector<UnqualifiedUnboundField> fields, const std::string& name = "result")
    {
        const auto sinkSchema = fields | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();
        const std::unordered_map<Identifier, std::string> sinkConfig{
            {Identifier::parse("FILE_PATH"), "/dev/null"}, {Identifier::parse("OUTPUT_FORMAT"), "CSV"}};
        const auto descriptor
            = sinkCatalog
                  .addSinkDescriptor(Identifier::parse(name), sinkSchema, Identifier::parse("file"), Host{"localhost"}, sinkConfig, {})
                  .value();
        return SinkLogicalOperator::create(descriptor).withChildrenUnsafe({child});
    }

    static std::vector<std::string> orderedNames(const TypedLogicalOperator<SemanticFilterLogicalOperator>& filter)
    {
        const auto ordered = filter->getOrderedOutputSchema([](const LogicalOperator& op)
                                                            { return op.getOutputSchema() | std::ranges::to<Schema<Field, Ordered>>(); });
        std::vector<std::string> names;
        for (const auto& orderedField : ordered)
        {
            names.push_back(fmt::format("{}", orderedField.getLastName()));
        }
        return names;
    }
};

}

/// A filter only drops records: its schema is the child's, field for field.
TEST_F(SemanticFilterLogicalOperatorTest, PassesTheChildSchemaThrough)
{
    const auto child = source("reviews", {field("description", DataType::Type::VARSIZED), field("id", DataType::Type::UINT64)});
    const TypedLogicalOperator<SemanticFilterLogicalOperator> filter{filterModel(semanticModels, "f"), LogicalOperator{child}};

    const auto schema = filter->getOutputSchema();
    EXPECT_EQ(schema.size(), 2);
    EXPECT_TRUE(schema[Identifier::parse("description")].has_value());
    EXPECT_TRUE(schema[Identifier::parse("id")].has_value());
    EXPECT_EQ(orderedNames(filter), (std::vector<std::string>{"DESCRIPTION", "ID"}));
    EXPECT_EQ(filter->getName(), "SemanticFilter");
    EXPECT_EQ(filter->explain(ExplainVerbosity::Short, OperatorId{1}), "SEM_FILTER(model: f, inputFields: [DESCRIPTION])");
}

/// A filter fused with a map appends the map's column, exactly as the map alone would have.
TEST_F(SemanticFilterLogicalOperatorTest, FusedMapStepsAppendTheirColumns)
{
    const auto child = source("reviews", {field("description", DataType::Type::VARSIZED)});
    const auto fused = fuseSemanticModels(mapModel(semanticModels, "m", "sentiment"), filterModel(semanticModels, "f"));
    const TypedLogicalOperator<SemanticFilterLogicalOperator> filter{fused, LogicalOperator{child}};

    const auto sentiment = filter->getOutputSchema()[Identifier::parse("sentiment")];
    ASSERT_TRUE(sentiment.has_value());
    EXPECT_EQ(sentiment->getDataType().type, DataType::Type::VARSIZED);
    EXPECT_EQ(orderedNames(filter), (std::vector<std::string>{"DESCRIPTION", "SENTIMENT"}));
    EXPECT_NE(filter->explain(ExplainVerbosity::Short, OperatorId{1}).find("model: m+f"), std::string::npos);
}

TEST_F(SemanticFilterLogicalOperatorTest, RejectsChildrenThatDoNotFitTheModel)
{
    const auto model = filterModel(semanticModels, "f");
    const auto rejects = [&](std::string_view name, std::vector<UnqualifiedUnboundField> fields)
    {
        SCOPED_TRACE(name);
        const auto child = source(name, std::move(fields));
        ASSERT_EXCEPTION_ERRORCODE(
            (TypedLogicalOperator<SemanticFilterLogicalOperator>{model, LogicalOperator{child}}), ErrorCode::CannotInferSchema);
    };
    rejects("missingInput", {field("other", DataType::Type::VARSIZED)});
    rejects("nullableInput", {field("description", DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE)});
    rejects("numericInput", {field("description", DataType::Type::UINT64)});
}

TEST_F(SemanticFilterLogicalOperatorTest, NamePlaceholderCanBeRebuilt)
{
    const auto first = source("first", {field("description", DataType::Type::VARSIZED)});
    const auto second = source("second", {field("description", DataType::Type::VARSIZED)});
    const TypedLogicalOperator<SemanticFilterNameLogicalOperator> placeholder{std::string{"F"}, LogicalOperator{first}};

    const auto rebuilt = LogicalOperator{placeholder}.withChildren({LogicalOperator{second}});
    ASSERT_TRUE(rebuilt.tryGetAs<SemanticFilterNameLogicalOperator>().has_value());
    EXPECT_EQ(rebuilt.getChildren().at(0), LogicalOperator{second});
    EXPECT_EQ(rebuilt.getAs<SemanticFilterNameLogicalOperator>()->getModelName(), "F");
    EXPECT_EQ(placeholder->explain(ExplainVerbosity::Short, OperatorId{1}), "SEM_FILTER_NAME(model: F)");
}

/// The coordinator -> worker path, for a plain filter and for a fused one.
TEST_F(SemanticFilterLogicalOperatorTest, SerializationRoundTrip)
{
    const auto plain = filterModel(semanticModels, "f");
    const auto fused = fuseSemanticModels(mapModel(semanticModels, "m", "sentiment"), plain);
    for (const auto& [model, sinkFields] :
         {std::pair{plain, std::vector{field("description", DataType::Type::VARSIZED)}},
          std::pair{fused, std::vector{field("description", DataType::Type::VARSIZED), field("sentiment", DataType::Type::VARSIZED)}}})
    {
        SCOPED_TRACE(model.getName());
        const auto child = source("reviews_" + std::to_string(sinkFields.size()), {field("description", DataType::Type::VARSIZED)});
        const TypedLogicalOperator<SemanticFilterLogicalOperator> filter{model, LogicalOperator{child}};
        const LogicalPlan plan{
            QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()),
            {sinkAbove(LogicalOperator{filter}, sinkFields, "result_" + std::to_string(sinkFields.size()))->withInferredSchema()}};

        const auto restored = QueryPlanSerializationUtil::deserializeQueryPlan(QueryPlanSerializationUtil::serializeQueryPlan(plan));

        const auto restoredFilters = getOperatorByType<SemanticFilterLogicalOperator>(restored);
        ASSERT_EQ(restoredFilters.size(), 1);
        EXPECT_EQ(restoredFilters.front()->getModel(), model);
        EXPECT_EQ(restoredFilters.front()->getOutputSchema().size(), sinkFields.size());
    }
}

/// NOLINTEND(readability-magic-numbers, bugprone-unchecked-optional-access)

}
