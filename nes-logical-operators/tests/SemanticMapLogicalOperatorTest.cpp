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
#include <Operators/SemanticMapLogicalOperator.hpp>
#include <Operators/SemanticMapNameLogicalOperator.hpp>
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

/// A model reading `description` and producing `outputs`, one step per output.
RegisteredSemanticModel loadModel(const std::string& name, const std::vector<std::string>& outputs = {"sentiment"})
{
    SemanticModelConfig config;
    config.backend = "mock";
    config.endpoint = "echo";
    config.modelName = "mock-model";
    std::vector<UnqualifiedUnboundField> outputFields;
    for (const auto& output : outputs)
    {
        config.steps.push_back(SemanticStep{
            .kind = SemanticStep::Kind::MAP,
            .prompt = "classify",
            .outputColumn = output,
            .outputValues = {"A", "B"},
            .defaultValue = "A"});
        outputFields.push_back(field(output, DataType::Type::VARSIZED));
    }
    SemanticModelCatalog catalog;
    catalog.registerModel(
        name,
        std::move(config),
        SemanticModelSchema{
            .inputs = SemanticFieldList{field("description", DataType::Type::VARSIZED)},
            .outputs = std::move(outputFields) | std::ranges::to<SemanticFieldList>()});
    return catalog.load(name);
}

class SemanticMapLogicalOperatorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SemanticMapLogicalOperatorTest.log", LogLevel::LOG_DEBUG); }

    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;

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

    /// A file sink over (description, sentiment), linked to `child` without inference.
    TypedLogicalOperator<SinkLogicalOperator> sinkAbove(const LogicalOperator& child)
    {
        const Schema<UnqualifiedUnboundField, Ordered> sinkSchema{
            field("description", DataType::Type::VARSIZED), field("sentiment", DataType::Type::VARSIZED)};
        const std::unordered_map<Identifier, std::string> sinkConfig{
            {Identifier::parse("FILE_PATH"), "/dev/null"}, {Identifier::parse("OUTPUT_FORMAT"), "CSV"}};
        const auto descriptor
            = sinkCatalog
                  .addSinkDescriptor(Identifier::parse("result"), sinkSchema, Identifier::parse("file"), Host{"localhost"}, sinkConfig, {})
                  .value();
        return SinkLogicalOperator::create(descriptor).withChildrenUnsafe({child});
    }
};

}

TEST_F(SemanticMapLogicalOperatorTest, AppendsOneOutputFieldAndPassesTheRestThrough)
{
    const auto child = source("reviews", {field("description", DataType::Type::VARSIZED), field("id", DataType::Type::UINT64)});
    const TypedLogicalOperator<SemanticMapLogicalOperator> semanticMap{loadModel("m"), LogicalOperator{child}};

    const auto schema = semanticMap->getOutputSchema();
    EXPECT_EQ(schema.size(), 3);
    EXPECT_TRUE(schema[Identifier::parse("description")].has_value());
    EXPECT_TRUE(schema[Identifier::parse("id")].has_value());
    const auto sentiment = schema[Identifier::parse("sentiment")];
    ASSERT_TRUE(sentiment.has_value());
    EXPECT_EQ(sentiment->getDataType().type, DataType::Type::VARSIZED);
    EXPECT_FALSE(sentiment->getDataType().nullable);
    EXPECT_EQ(semanticMap->getName(), "SemanticMap");
    EXPECT_NE(semanticMap->explain(ExplainVerbosity::Short, OperatorId{1}).find("DESCRIPTION"), std::string::npos);
}

/// Output fields follow the declared step order, not the default lexicographic order.
TEST_F(SemanticMapLogicalOperatorTest, OrderedOutputSchemaKeepsDeclaredStepOrder)
{
    const auto child = source("reviews", {field("description", DataType::Type::VARSIZED)});
    const TypedLogicalOperator<SemanticMapLogicalOperator> semanticMap{loadModel("m", {"zulu", "alpha"}), LogicalOperator{child}};

    const auto ordered = semanticMap->getOrderedOutputSchema([](const LogicalOperator& op)
                                                             { return op.getOutputSchema() | std::ranges::to<Schema<Field, Ordered>>(); });
    std::vector<std::string> names;
    for (const auto& orderedField : ordered)
    {
        names.push_back(fmt::format("{}", orderedField.getLastName()));
    }
    EXPECT_EQ(names, (std::vector<std::string>{"DESCRIPTION", "ZULU", "ALPHA"}));
}

TEST_F(SemanticMapLogicalOperatorTest, RejectsChildrenThatDoNotFitTheModel)
{
    const auto model = loadModel("m");
    const auto rejects = [&](std::string_view name, std::vector<UnqualifiedUnboundField> fields)
    {
        SCOPED_TRACE(name);
        const auto child = source(name, std::move(fields));
        ASSERT_EXCEPTION_ERRORCODE(
            (TypedLogicalOperator<SemanticMapLogicalOperator>{model, LogicalOperator{child}}), ErrorCode::CannotInferSchema);
    };
    rejects("missingInput", {field("other", DataType::Type::VARSIZED)});
    rejects("nullableInput", {field("description", DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE)});
    rejects("numericInput", {field("description", DataType::Type::UINT64)});
    rejects("outputCollision", {field("description", DataType::Type::VARSIZED), field("sentiment", DataType::Type::VARSIZED)});
}

/// withChildrenUnsafe relinks without inference (what SemanticMapResolutionRule relies on);
/// withInferredSchema supplies the schema afterwards.
TEST_F(SemanticMapLogicalOperatorTest, WithChildrenUnsafeDefersInference)
{
    const auto child = source("reviews", {field("description", DataType::Type::VARSIZED)});
    const auto relinked
        = LogicalOperator{TypedLogicalOperator<SemanticMapLogicalOperator>{loadModel("m")}}.withChildrenUnsafe({LogicalOperator{child}});
    ASSERT_EQ(relinked.getChildren().size(), 1);
    EXPECT_TRUE(relinked.withInferredSchema().getOutputSchema()[Identifier::parse("sentiment")].has_value());
}

/// The placeholder's withChildren must be functional: generic rewriting rules rebuild every operator
/// they walk over, and a guard there used to take the whole process down.
TEST_F(SemanticMapLogicalOperatorTest, NamePlaceholderCanBeRebuilt)
{
    const auto first = source("first", {field("description", DataType::Type::VARSIZED)});
    const auto second = source("second", {field("description", DataType::Type::VARSIZED)});
    const TypedLogicalOperator<SemanticMapNameLogicalOperator> placeholder{std::string{"M"}, LogicalOperator{first}};

    const auto rebuilt = LogicalOperator{placeholder}.withChildren({LogicalOperator{second}});
    ASSERT_TRUE(rebuilt.tryGetAs<SemanticMapNameLogicalOperator>().has_value());
    EXPECT_EQ(rebuilt.getChildren().at(0), LogicalOperator{second});
    EXPECT_EQ(rebuilt.getAs<SemanticMapNameLogicalOperator>()->getModelName(), "M");
    EXPECT_EQ(placeholder->explain(ExplainVerbosity::Short, OperatorId{1}), "SEM_MAP_NAME(model: M)");
}

/// The coordinator -> worker path: the resolved operator, including its whole catalog entry,
/// survives plan serialization.
TEST_F(SemanticMapLogicalOperatorTest, SerializationRoundTrip)
{
    const auto child = source("reviews", {field("description", DataType::Type::VARSIZED)});
    const auto model = loadModel("m");
    const TypedLogicalOperator<SemanticMapLogicalOperator> semanticMap{model, LogicalOperator{child}};
    const LogicalPlan plan{
        QueryId::create(LocalQueryId{generateUUID()}, getNextDistributedQueryId()),
        {sinkAbove(LogicalOperator{semanticMap})->withInferredSchema()}};

    const auto restored = QueryPlanSerializationUtil::deserializeQueryPlan(QueryPlanSerializationUtil::serializeQueryPlan(plan));

    const auto restoredMaps = getOperatorByType<SemanticMapLogicalOperator>(restored);
    ASSERT_EQ(restoredMaps.size(), 1);
    EXPECT_EQ(restoredMaps.front()->getModel(), model);
    EXPECT_TRUE(restoredMaps.front()->getOutputSchema()[Identifier::parse("sentiment")].has_value());
}

/// NOLINTEND(readability-magic-numbers, bugprone-unchecked-optional-access)

}
