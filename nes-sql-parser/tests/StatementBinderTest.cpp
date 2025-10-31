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
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>


#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Sinks/InlineSinkLogicalOperator.hpp>
#include <Operators/Sources/InlineSourceLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/FileSink.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

class StatementBinderTest : public Testing::BaseUnitTest
{
public:
    std::shared_ptr<SourceCatalog> sourceCatalog;
    std::shared_ptr<SinkCatalog> sinkCatalog;
    std::shared_ptr<StatementBinder> binder;
    std::shared_ptr<SourceStatementHandler> sourceStatementHandler;
    std::shared_ptr<SinkStatementHandler> sinkStatementHandler;

    /* Will be called before a test is executed. */
    static void SetUpTestSuite()
    {
        Logger::setupLogging("StatementBinderTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatementBinderTest test case.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        sourceCatalog = std::make_shared<SourceCatalog>();
        sinkCatalog = std::make_shared<SinkCatalog>();
        binder = std::make_shared<StatementBinder>(
            sourceCatalog,
            [](auto&& queryContext)
            { return AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(queryContext)>(queryContext)); });
        sourceStatementHandler = std::make_shared<SourceStatementHandler>(sourceCatalog);
        sinkStatementHandler = std::make_shared<SinkStatementHandler>(sinkCatalog);
    }
};

///NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST_F(StatementBinderTest, BindQuery)
{
    /// This test only checks that the input was detected as a query, for more detailed testing of the QueryBinder
    /// see the AntlrSQLQueryParserTest and the Systests
    /// In the future this will require setting up the source catalog correctly as well
    const std::string queryString = "SELECT a FROM inputStream WHERE b < UINT32(5) INTO outputStream";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));
}

TEST_F(StatementBinderTest, InlineSinkQuery)
{
    const std::string query = "SELECT id, text \n"
                              "FROM input\n"
                              "INTO FILE(\n"
                              "'out.csv' AS `SINK`.FILE_PATH,\n"
                              "'CSV' as `SINK`.INPUT_FORMAT,\n"
                              "SCHEMA(id UINT64, text VARSIZED) AS `SINK`.`SCHEMA`)\n";
    const auto statement = binder->parseAndBindSingle(query);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;

    const auto inlineSinkOperatorList = getOperatorByType<InlineSinkLogicalOperator>(plan);
    ASSERT_EQ(1, inlineSinkOperatorList.size());

    const auto& inlineSinkOperator = inlineSinkOperatorList.at(0);

    ASSERT_EQ("FILE", inlineSinkOperator->getSinkType());

    const std::unordered_map<std::string, std::string> expectedSinkConfig = {{"file_path", "out.csv"}, {"input_format", "CSV"}};
    ASSERT_EQ(expectedSinkConfig, inlineSinkOperator->getSinkConfig());

    Schema schema;
    schema.addField("ID", DataTypeProvider::provideDataType(DataType::Type::UINT64));
    schema.addField("TEXT", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    ASSERT_EQ(schema, inlineSinkOperator->getSchema());
}

TEST_F(StatementBinderTest, InlineSourceQuery)
{
    const std::string query = "SELECT id, text \n"
                              "FROM File(\n"
                              "'input.csv' AS `SOURCE`.FILE_PATH,\n"
                              "'CSV' AS PARSER.`TYPE`,\n"
                              "SCHEMA(id UINT64, text VARSIZED) AS `SOURCE`.`SCHEMA`)\n"
                              "INTO output\n";
    const auto statement = binder->parseAndBindSingle(query);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<QueryStatement>(*statement));

    const auto plan = std::get<QueryStatement>(*statement).plan;

    const auto inlineSourceOperatorList = getOperatorByType<InlineSourceLogicalOperator>(plan);
    ASSERT_EQ(1, inlineSourceOperatorList.size());

    const auto& inlineSourceOperator = inlineSourceOperatorList.at(0);

    ASSERT_EQ("FILE", inlineSourceOperator->getSourceType());

    const std::unordered_map<std::string, std::string> expectedSourceConfig = {{"file_path", "input.csv"}};
    ASSERT_EQ(expectedSourceConfig, inlineSourceOperator->getSourceConfig());

    const std::unordered_map<std::string, std::string> expectedParserConfig = {{"type", "CSV"}};
    ASSERT_EQ(expectedParserConfig, inlineSourceOperator->getParserConfig());

    Schema schema;
    schema.addField("ID", DataTypeProvider::provideDataType(DataType::Type::UINT64));
    schema.addField("TEXT", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    ASSERT_EQ(schema, inlineSourceOperator->getSchema());
}

TEST_F(StatementBinderTest, BindQuotedIdentifiers)
{
    const std::string createLogicalSourceStatement = "CREATE LOGICAL SOURCE `testSource` (`attribute1` UINT32, `attribute2` VARSIZED)";
    const auto statement1 = binder->parseAndBindSingle(createLogicalSourceStatement);
    ASSERT_TRUE(statement1.has_value());
    ASSERT_TRUE(std::holds_alternative<CreateLogicalSourceStatement>(*statement1));
    const auto createdSourceResult = sourceStatementHandler->apply(std::get<CreateLogicalSourceStatement>(*statement1));
    ASSERT_TRUE(createdSourceResult.has_value());
    const auto [actualSource] = createdSourceResult.value();
    Schema expectedSchema{};
    auto expectedColumns = std::vector<std::pair<std::string, std::shared_ptr<DataType>>>{};
    expectedSchema.addField("testSource$attribute1", DataTypeProvider::provideDataType(DataType::Type::UINT32));
    expectedSchema.addField("testSource$attribute2", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    ASSERT_EQ(actualSource.getLogicalSourceName(), "testSource");
    ASSERT_EQ(*actualSource.getSchema(), expectedSchema);
}

TEST_F(StatementBinderTest, BindCreateBindSource)
{
    const std::string createLogicalSourceStatement = "CREATE LOGICAL SOURCE testSource (attribute1 UINT32, attribute2 VARSIZED)";
    const auto statement1 = binder->parseAndBindSingle(createLogicalSourceStatement);
    ASSERT_TRUE(statement1.has_value());
    ASSERT_TRUE(std::holds_alternative<CreateLogicalSourceStatement>(*statement1));
    const auto createdSourceResult = sourceStatementHandler->apply(std::get<CreateLogicalSourceStatement>(*statement1));
    ASSERT_TRUE(createdSourceResult.has_value());
    const auto [actualSource] = createdSourceResult.value();
    Schema expectedSchema{};
    auto expectedColumns = std::vector<std::pair<std::string, std::shared_ptr<DataType>>>{};
    expectedSchema.addField("TESTSOURCE$ATTRIBUTE1", DataTypeProvider::provideDataType(DataType::Type::UINT32));
    expectedSchema.addField("TESTSOURCE$ATTRIBUTE2", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    ASSERT_EQ(actualSource.getLogicalSourceName(), "TESTSOURCE");
    ASSERT_EQ(*actualSource.getSchema(), expectedSchema);

    const std::string createPhysicalSourceStatement
        = R"(CREATE PHYSICAL SOURCE FOR testSource TYPE File SET (0 as `SOURCE`.MAX_INFLIGHT_BUFFERS, '/dev/null' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER))";
    const auto statement2 = binder->parseAndBindSingle(createPhysicalSourceStatement);
    const ParserConfig expectedParserConfig{.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","};
    std::unordered_map<std::string, std::string> unvalidatedConfig{{"file_path", "/dev/null"}};
    const DescriptorConfig::Config descriptorConfig = SourceValidationProvider::provide("File", std::move(unvalidatedConfig)).value();

    ASSERT_TRUE(statement2.has_value());
    ASSERT_TRUE(std::holds_alternative<CreatePhysicalSourceStatement>(*statement2));
    const auto physicalSourceResult = sourceStatementHandler->apply(std::get<CreatePhysicalSourceStatement>(*statement2));
    ASSERT_TRUE(physicalSourceResult.has_value());
    const auto [physicalSource] = physicalSourceResult.value();
    ASSERT_EQ(physicalSource.getLogicalSource(), actualSource);
    ASSERT_EQ(physicalSource.getParserConfig(), expectedParserConfig);
    ASSERT_EQ(physicalSource.getSourceType(), "File");
    ASSERT_EQ(physicalSource.getConfig(), descriptorConfig);
    ASSERT_EQ(physicalSource.getPhysicalSourceId().getRawValue(), 1);

    const std::string dropPhysicalSourceStatement = "DROP PHYSICAL SOURCE 1";
    const auto statement3 = binder->parseAndBindSingle(dropPhysicalSourceStatement);
    ASSERT_TRUE(statement3.has_value());
    ASSERT_TRUE(std::holds_alternative<DropPhysicalSourceStatement>(*statement3));
    const auto droppedResult = sourceStatementHandler->apply(std::get<DropPhysicalSourceStatement>(*statement3));
    ASSERT_TRUE(droppedResult.has_value());
    const auto [dropped] = droppedResult.value();
    ASSERT_EQ(dropped.getPhysicalSourceId().getRawValue(), 1);
    auto remainingPhysicalSources = sourceCatalog->getPhysicalSources(actualSource);
    ASSERT_TRUE(remainingPhysicalSources.has_value());
    ASSERT_EQ(remainingPhysicalSources.value().size(), 0);

    const std::string dropLogicalSourceStatement = "DROP LOGICAL SOURCE testSource";
    const auto statement4 = binder->parseAndBindSingle(dropLogicalSourceStatement);
    ASSERT_TRUE(statement4.has_value());
    ASSERT_TRUE(std::holds_alternative<DropLogicalSourceStatement>(*statement4));
    const auto dropped2Result = sourceStatementHandler->apply(std::get<DropLogicalSourceStatement>(*statement4));
    ASSERT_TRUE(dropped2Result.has_value());
    const auto [sourceName, schema] = dropped2Result.value();
    ASSERT_EQ(sourceName.getRawValue(), "TESTSOURCE");
    auto remainingLogicalSources = sourceCatalog->getLogicalToPhysicalSourceMapping();
    ASSERT_EQ(remainingLogicalSources.size(), 0);
}

TEST_F(StatementBinderTest, BindCreateBindSourceWithInvalidConfigs)
{
    Schema schema{};
    auto expectedColumns = std::vector<std::pair<std::string, std::shared_ptr<DataType>>>{};
    schema.addField("ATTRIBUTE1", DataTypeProvider::provideDataType(DataType::Type::UINT32));
    schema.addField("ATTRIBUTE2", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    const auto logicalSourceOpt = sourceCatalog->addLogicalSource("TESTSOURCE", schema);
    ASSERT_TRUE(logicalSourceOpt.has_value());
}

TEST_F(StatementBinderTest, BindCreateSink)
{
    const std::string createSinkStatement = "CREATE SINK testSink (attribute1 UINT32, attribute2 VARSIZED) TYPE File SET ('/dev/null' AS "
                                            "`SINK`.FILE_PATH, 'CSV' AS `SINK`.INPUT_FORMAT)";
    const auto statement = binder->parseAndBindSingle(createSinkStatement);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<CreateSinkStatement>(*statement));
    const auto createdSinkResult = sinkStatementHandler->apply(std::get<CreateSinkStatement>(*statement));
    ASSERT_TRUE(createdSinkResult.has_value());
    const auto [actualSink] = createdSinkResult.value();
    Schema expectedSchema{};
    expectedSchema.addField("ATTRIBUTE1", DataTypeProvider::provideDataType(DataType::Type::UINT32));
    expectedSchema.addField("ATTRIBUTE2", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    ASSERT_EQ(actualSink.getSinkName(), "TESTSINK");
    ASSERT_EQ(*actualSink.getSchema(), expectedSchema);
    ASSERT_EQ(actualSink.getSinkType(), "File");
    ASSERT_EQ(actualSink.getFromConfig(SinkDescriptor::FILE_PATH), "/dev/null");
    ASSERT_EQ(actualSink.getFromConfig(SinkDescriptor::INPUT_FORMAT), InputFormat::CSV);

    const std::string dropSinkStatement = "DROP SINK testSink";
    const auto statement2 = binder->parseAndBindSingle(dropSinkStatement);
    ASSERT_TRUE(statement2.has_value());
    ASSERT_TRUE(std::holds_alternative<DropSinkStatement>(*statement2));
    const auto droppedResult = sinkStatementHandler->apply(std::get<DropSinkStatement>(*statement2));
    ASSERT_TRUE(droppedResult.has_value());
    const auto [dropped] = droppedResult.value();
    ASSERT_EQ(dropped.getSinkName(), "TESTSINK");
    auto remainingSinks = sinkCatalog->getAllSinkDescriptors();
    ASSERT_EQ(remainingSinks.size(), 0);
}

/// TODO #764 Remove test when we have proper schema inference and don't require matching source names in sinks anymore
TEST_F(StatementBinderTest, BindCreateSinkWithQualifiedColumns)
{
    const std::string createSinkStatement = "CREATE SINK testSink (testSource.attribute1 UINT32, testSource.attribute2 VARSIZED) TYPE File "
                                            "SET ('/dev/null' AS `SINK`.FILE_PATH, 'CSV' AS `SINK`.INPUT_FORMAT)";
    const auto statement = binder->parseAndBindSingle(createSinkStatement);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<CreateSinkStatement>(*statement));
    const auto createdSinkResult = sinkStatementHandler->apply(std::get<CreateSinkStatement>(*statement));
    ASSERT_TRUE(createdSinkResult.has_value());
    const auto [actualSink] = createdSinkResult.value();
    Schema expectedSchema{};
    expectedSchema.addField("TESTSOURCE$ATTRIBUTE1", DataTypeProvider::provideDataType(DataType::Type::UINT32));
    expectedSchema.addField("TESTSOURCE$ATTRIBUTE2", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    ASSERT_EQ(actualSink.getSinkName(), "TESTSINK");
    ASSERT_EQ(*actualSink.getSchema(), expectedSchema);
    ASSERT_EQ(actualSink.getSinkType(), "File");
    ASSERT_EQ(actualSink.getFromConfig(SinkDescriptor::FILE_PATH), "/dev/null");
    ASSERT_EQ(actualSink.getFromConfig(SinkDescriptor::INPUT_FORMAT), InputFormat::CSV);
}

TEST_F(StatementBinderTest, BindDropQuery)
{
    const std::string queryString = "DROP QUERY 12";
    const auto statement = binder->parseAndBindSingle(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<DropQueryStatement>(*statement));
    ASSERT_EQ(std::get<DropQueryStatement>(*statement).id.getRawValue(), 12);

    const std::string queryString2 = "DROP QUERY -5";
    const auto statement2 = binder->parseAndBindSingle(queryString2);
    ASSERT_FALSE(statement2.has_value());
    ASSERT_EQ(statement2.error().code(), ErrorCode::InvalidQuerySyntax);
}

TEST_F(StatementBinderTest, ShowLogicalSources)
{
    const std::vector<std::string_view> createLogicalSources{
        "CREATE LOGICAL SOURCE testSource1 (attribute1 UINT32, attribute2 VARSIZED)",
        "CREATE LOGICAL SOURCE testSource2 (attribute1 UINT32, attribute2 INT32)"};

    for (const auto& createLogicalSource : createLogicalSources)
    {
        const auto statement = binder->parseAndBindSingle(createLogicalSource);
        ASSERT_TRUE(statement.has_value());
        ASSERT_TRUE(sourceStatementHandler->apply(std::get<CreateLogicalSourceStatement>(statement.value())).has_value());
    }

    const std::string allLogicalQueryString = "SHOW LOGICAL SOURCES FORMAT JSON";
    const std::string filteredQuotedLogicalQueryString = "SHOW LOGICAL SOURCES WHERE NAME = 'TESTSOURCE1'";
    const std::string invalidFormatQueryString = "SHOW LOGICAL SOURCES WHERE NAME = 'testSource1' FORMAT INVALID_FORMAT";
    const std::string formatInInvalidPositionString = "SHOW LOGICAL SOURCES FORMAT JSON WHERE NAME = 'testSource' ";

    const auto allSourcesStatementExp = binder->parseAndBindSingle(allLogicalQueryString);
    ASSERT_TRUE(allSourcesStatementExp.has_value());
    ASSERT_TRUE(std::holds_alternative<ShowLogicalSourcesStatement>(*allSourcesStatementExp));
    const auto [name, format] = std::get<ShowLogicalSourcesStatement>(*allSourcesStatementExp);
    ASSERT_FALSE(name.has_value());
    ASSERT_TRUE(format == StatementOutputFormat::JSON);
    const auto allSourcesStatementResult = sourceStatementHandler->apply(std::get<ShowLogicalSourcesStatement>(*allSourcesStatementExp));
    ASSERT_TRUE(allSourcesStatementResult.has_value());
    ASSERT_EQ(allSourcesStatementResult.value().sources.size(), 2);

    const auto filteredQuotedStatementExp = binder->parseAndBindSingle(filteredQuotedLogicalQueryString);
    ASSERT_TRUE(filteredQuotedStatementExp.has_value());
    ASSERT_TRUE(std::holds_alternative<ShowLogicalSourcesStatement>(*filteredQuotedStatementExp));
    const auto [name2, format2] = std::get<ShowLogicalSourcesStatement>(*filteredQuotedStatementExp);
    ASSERT_TRUE(name2.has_value());
    ASSERT_EQ(*name2, "TESTSOURCE1");
    ASSERT_TRUE(format2 == std::nullopt);
    const auto filteredQuotedSourcesStatementResult
        = sourceStatementHandler->apply(std::get<ShowLogicalSourcesStatement>(*filteredQuotedStatementExp));
    ASSERT_TRUE(filteredQuotedSourcesStatementResult.has_value());
    ASSERT_EQ(filteredQuotedSourcesStatementResult.value().sources.size(), 1);
    ASSERT_EQ(filteredQuotedSourcesStatementResult.value().sources.at(0).getLogicalSourceName(), "TESTSOURCE1");

    const auto invalidFormatStatementExp = binder->parseAndBindSingle(invalidFormatQueryString);
    ASSERT_FALSE(invalidFormatStatementExp.has_value());

    const auto formatInInvalidPositionStatementExp = binder->parseAndBindSingle(formatInInvalidPositionString);
    ASSERT_FALSE(formatInInvalidPositionStatementExp.has_value());
}

TEST_F(StatementBinderTest, ShowPhysicalSources)
{
    std::vector<std::string_view> createSourcesStatements;
    createSourcesStatements.emplace_back("CREATE LOGICAL SOURCE testSource1 (attribute1 UINT32, attribute2 VARSIZED)");
    createSourcesStatements.emplace_back("CREATE LOGICAL SOURCE testSource2 (attribute1 UINT32, attribute2 INT32)");
    createSourcesStatements.emplace_back(
        "CREATE PHYSICAL SOURCE FOR testSource1 TYPE File SET (200 as "
        "`SOURCE`.MAX_INFLIGHT_BUFFERS, '/dev/null' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS "
        "PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER)");
    createSourcesStatements.emplace_back(
        "CREATE PHYSICAL SOURCE FOR testSource2 TYPE File SET (0 as "
        "`SOURCE`.MAX_INFLIGHT_BUFFERS, '/dev/random' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS "
        "PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER)");
    createSourcesStatements.emplace_back(
        "CREATE PHYSICAL SOURCE FOR testSource2 TYPE File SET (0 as "
        "`SOURCE`.MAX_INFLIGHT_BUFFERS, '/dev/ones' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS "
        "PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER)");

    for (const auto& sourceStatementString : createSourcesStatements)
    {
        const auto boundStatement = binder->parseAndBindSingle(sourceStatementString);
        ASSERT_TRUE(boundStatement.has_value());
        std::visit(
            [this](auto& statement)
            {
                if constexpr (requires { sourceStatementHandler->apply(statement); })
                {
                    sourceStatementHandler->apply(statement);
                    return;
                }
                INVARIANT(false, "Invalid statement type");
            },
            boundStatement.value());
    }

    const std::string_view allPhysicalSourcesStatementString = "SHOW PHYSICAL SOURCES";
    const std::string_view filteredPhysicalSourcesStatementString = "SHOW PHYSICAL SOURCES WHERE ID = 2 FORMAT JSON";
    const std::string_view physicalSourceForLogicalSourceStatementString = "SHOW PHYSICAL SOURCES FOR testSourCe1 FORMAT TEXT";
    const std::string_view physicalSourceForLogicalSourceStatementFilteredString = "SHOW PHYSICAL SOURCES FOR testSource2 WHERE ID = 3";

    const auto allPhysicalSourcesStatementExp = binder->parseAndBindSingle(allPhysicalSourcesStatementString);
    ASSERT_TRUE(allPhysicalSourcesStatementExp.has_value());
    ASSERT_TRUE(std::holds_alternative<ShowPhysicalSourcesStatement>(*allPhysicalSourcesStatementExp));
    auto showAllPhysicalSources = std::get<ShowPhysicalSourcesStatement>(*allPhysicalSourcesStatementExp);
    const auto [logicalSource, id, format] = showAllPhysicalSources;
    ASSERT_FALSE(logicalSource.has_value());
    ASSERT_FALSE(id.has_value());
    ASSERT_TRUE(format == std::nullopt);
    const auto allPhysicalSourcesStatementResult = sourceStatementHandler->apply(showAllPhysicalSources);
    ASSERT_TRUE(allPhysicalSourcesStatementResult.has_value());
    ASSERT_EQ(allPhysicalSourcesStatementResult.value().sources.size(), 3);

    const auto filteredPhysicalSourcesStatementExp = binder->parseAndBindSingle(filteredPhysicalSourcesStatementString);
    ASSERT_TRUE(filteredPhysicalSourcesStatementExp.has_value());
    ASSERT_TRUE(std::holds_alternative<ShowPhysicalSourcesStatement>(*filteredPhysicalSourcesStatementExp));
    auto showFilteredPhysicalSources = std::get<ShowPhysicalSourcesStatement>(*filteredPhysicalSourcesStatementExp);
    const auto [logicalSource2, id2, format2] = showFilteredPhysicalSources;
    ASSERT_FALSE(logicalSource2.has_value());
    ASSERT_TRUE(id2.has_value());
    ASSERT_TRUE(*id2 == 2);
    ASSERT_TRUE(format2 == StatementOutputFormat::JSON);
    const auto filteredPhysicalSourcesStatementResult = sourceStatementHandler->apply(showFilteredPhysicalSources);
    ASSERT_TRUE(filteredPhysicalSourcesStatementResult.has_value());
    ASSERT_EQ(filteredPhysicalSourcesStatementResult.value().sources.size(), 1);
    ASSERT_EQ(
        filteredPhysicalSourcesStatementResult.value().sources.at(0).tryGetFromConfig<std::string>("file_path").value(), "/dev/random");

    const auto physicalSourceForLogicalSourceStatementExp = binder->parseAndBindSingle(physicalSourceForLogicalSourceStatementString);
    ASSERT_TRUE(physicalSourceForLogicalSourceStatementExp.has_value());
    ASSERT_TRUE(std::holds_alternative<ShowPhysicalSourcesStatement>(*physicalSourceForLogicalSourceStatementExp));
    auto showPhysicalSourceForLogicalSource = std::get<ShowPhysicalSourcesStatement>(*physicalSourceForLogicalSourceStatementExp);
    const auto [logicalSource3, id3, format3] = showPhysicalSourceForLogicalSource;
    ASSERT_TRUE(logicalSource3.has_value());
    ASSERT_EQ(logicalSource3->getRawValue(), "TESTSOURCE1");
    ASSERT_FALSE(id3.has_value());
    ASSERT_TRUE(format3 == StatementOutputFormat::TEXT);
    const auto physicalSourceForLogicalSourceStatementResult = sourceStatementHandler->apply(showPhysicalSourceForLogicalSource);
    ASSERT_TRUE(physicalSourceForLogicalSourceStatementResult.has_value());
    ASSERT_EQ(physicalSourceForLogicalSourceStatementResult.value().sources.size(), 1);
    ASSERT_EQ(
        physicalSourceForLogicalSourceStatementResult.value().sources.at(0).getFromConfig(SourceDescriptor::MAX_INFLIGHT_BUFFERS), 200);

    const auto physicalSourceForLogicalSourceStatementFilteredExp
        = binder->parseAndBindSingle(physicalSourceForLogicalSourceStatementFilteredString);
    ASSERT_TRUE(physicalSourceForLogicalSourceStatementFilteredExp.has_value());
    ASSERT_TRUE(std::holds_alternative<ShowPhysicalSourcesStatement>(*physicalSourceForLogicalSourceStatementFilteredExp));
    auto showPhysicalSourceForLogicalSourceFiltered
        = std::get<ShowPhysicalSourcesStatement>(*physicalSourceForLogicalSourceStatementFilteredExp);
    const auto [logicalSource4, id4, format4] = showPhysicalSourceForLogicalSourceFiltered;
    ASSERT_TRUE(logicalSource4.has_value());
    ASSERT_EQ(logicalSource4->getRawValue(), "TESTSOURCE2");
    ASSERT_TRUE(id4.has_value());
    ASSERT_TRUE(*id4 == 3);
    ASSERT_TRUE(format4 == std::nullopt);
    const auto physicalSourceForLogicalSourceStatementFilteredResult
        = sourceStatementHandler->apply(showPhysicalSourceForLogicalSourceFiltered);
    ASSERT_TRUE(physicalSourceForLogicalSourceStatementFilteredResult.has_value());
    ASSERT_EQ(physicalSourceForLogicalSourceStatementFilteredResult.value().sources.size(), 1);
    ASSERT_EQ(
        physicalSourceForLogicalSourceStatementFilteredResult.value().sources.at(0).tryGetFromConfig<std::string>("file_path").value(),
        "/dev/ones");
}

TEST_F(StatementBinderTest, ShowSinks)
{
    const std::vector<std::string_view> sinkStatements{
        "CREATE SINK testSink1 (attribute1 UINT32, attribute2 VARSIZED) TYPE File SET ('/dev/null' AS `SINK`.FILE_PATH, 'CSV' AS "
        "`SINK`.INPUT_FORMAT)",
        "CREATE SINK testSink2 (attribute1 UINT32, attribute2 INT32) TYPE File SET ('/dev/null' AS `SINK`.FILE_PATH, 'CSV' AS "
        "`SINK`.INPUT_FORMAT)"};

    for (const auto& sinkStatement : sinkStatements)
    {
        const auto statement = binder->parseAndBindSingle(sinkStatement);
        ASSERT_TRUE(statement.has_value());
        ASSERT_TRUE(sinkStatementHandler->apply(std::get<CreateSinkStatement>(statement.value())).has_value());
    }

    const std::string allSinksQueryString = "SHOW SINKS FORMAT JSON";
    const std::string filteredQuotedSinksQueryString = "SHOW SINKS WHERE NAME = 'TESTSINK1'";
    const std::string invalidFormatQueryString = "SHOW SINKS WHERE NAME = 'testSink1' FORMAT INVALID_FORMAT";

    const auto allSinksStatementExp = binder->parseAndBindSingle(allSinksQueryString);
    const auto filteredQuotedSinksStatementExp = binder->parseAndBindSingle(filteredQuotedSinksQueryString);
    const auto invalidFormatStatementExp = binder->parseAndBindSingle(invalidFormatQueryString);
    ASSERT_TRUE(allSinksStatementExp.has_value());
    ASSERT_TRUE(filteredQuotedSinksStatementExp.has_value());
    ASSERT_FALSE(invalidFormatStatementExp.has_value());

    auto allSinksStatement = std::get<ShowSinksStatement>(*allSinksStatementExp);
    const auto [name, format] = allSinksStatement;
    ASSERT_FALSE(name.has_value());
    ASSERT_TRUE(format == StatementOutputFormat::JSON);
    auto filteredQuotedSinksStatement = std::get<ShowSinksStatement>(*filteredQuotedSinksStatementExp);
    const auto [name2, format2] = filteredQuotedSinksStatement;
    ASSERT_TRUE(name2.has_value());
    ASSERT_EQ(*name2, "TESTSINK1");
    ASSERT_TRUE(format2 == std::nullopt);

    const auto allSinksResult = sinkStatementHandler->apply(allSinksStatement);
    const auto filteredQuotedSinksResult = sinkStatementHandler->apply(filteredQuotedSinksStatement);
    ASSERT_TRUE(allSinksResult.has_value());
    ASSERT_TRUE(filteredQuotedSinksResult.has_value());
    ASSERT_EQ(allSinksResult.value().sinks.size(), 2);
    ASSERT_EQ(filteredQuotedSinksResult.value().sinks.size(), 1);
    ASSERT_EQ(filteredQuotedSinksResult.value().sinks.at(0).getSinkName(), "TESTSINK1");
}

TEST_F(StatementBinderTest, ExplainStatement)
{
    const std::vector<std::string_view> validExplainStatement{
        "EXPLAIN SELECT * FROM `source` INTO `sink`", "explain SELECT * FROM `source` INTO `sink`"};
    const std::vector<std::string_view> matchingQueries{"SELECT * FROM `source` INTO `sink`", "SELECT * FROM `source` INTO `sink`"};

    for (const auto& [explain, query] : std::views::zip(validExplainStatement, matchingQueries))
    {
        const auto explainStatementResult = binder->parseAndBindSingle(explain);
        const auto queryStatementResult = binder->parseAndBindSingle(query);
        ASSERT_TRUE(explainStatementResult.has_value());
        ASSERT_TRUE(queryStatementResult.has_value());

        auto explainStatement = std::get<ExplainQueryStatement>(explainStatementResult.value());
        auto queryStatement = std::get<QueryStatement>(queryStatementResult.value());
        EXPECT_EQ(explainStatement.plan, queryStatement.plan);
    }

    const std::vector<std::string_view> badExplainStatement{
        "EXPLAIN CREATE SINK testSink1 (attribute1 UINT32, attribute2 VARSIZED) TYPE File SET ('/dev/null' AS `SINK`.FILE_PATH, 'CSV' AS "
        "`SINK`.INPUT_FORMAT)",
        "EXPLAIN SHOW PHYSICAL SOURCES"};

    for (const auto& explain : badExplainStatement)
    {
        const auto explainStatementResult = binder->parseAndBindSingle(explain);
        ASSERT_FALSE(explainStatementResult.has_value());
    }
}

///NOLINTEND(bugprone-unchecked-optional-access)
}
}
