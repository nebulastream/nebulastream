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
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
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
            sinkCatalog,
            [](auto&& queryContext)
            { return AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(queryContext)>(queryContext)); });
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

TEST_F(StatementBinderTest, BindCreateSourceStatements)
{
    const std::string createLogicalSourceStatement = "CREATE LOGICAL SOURCE testSource (attribute1 UINT32, attribute2 VARSIZED)";
    const auto statement1 = binder->parseAndBindSingle(createLogicalSourceStatement);
    ASSERT_TRUE(statement1.has_value());
    ASSERT_TRUE(std::holds_alternative<CreateLogicalSourceStatement>(*statement1));

    const auto& cls = std::get<CreateLogicalSourceStatement>(*statement1);
    ASSERT_EQ(cls.name, "TESTSOURCE");
    ASSERT_TRUE(cls.schema.contains("ATTRIBUTE1"));
    ASSERT_TRUE(cls.schema.contains("ATTRIBUTE2"));

    /// Provide the logical source to the catalog for subsequent bindings that require existence
    ASSERT_TRUE(sourceCatalog->addLogicalSource(cls.name, cls.schema).has_value());
    /// Register a physical source so that DROP PHYSICAL SOURCE 1 can bind to an existing descriptor
    auto physOpt = sourceCatalog->addPhysicalSource(
        sourceCatalog->getLogicalSource(cls.name).value(),
        "File",
        std::unordered_map<std::string, std::string>{{"filePath", "/dev/null"}},
        ParserConfig{});
    ASSERT_TRUE(physOpt.has_value());

    const std::string createPhysicalSourceStatement
        = R"(CREATE PHYSICAL SOURCE FOR testSource TYPE File SET (-1 as `SOURCE`.NUMBER_OF_BUFFERS_IN_LOCAL_POOL, '/dev/null' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER))";
    const auto statement2 = binder->parseAndBindSingle(createPhysicalSourceStatement);
    ASSERT_TRUE(statement2.has_value());
    ASSERT_TRUE(std::holds_alternative<CreatePhysicalSourceStatement>(*statement2));

    const auto& cps = std::get<CreatePhysicalSourceStatement>(*statement2);
    ASSERT_EQ(cps.attachedTo.getLogicalSourceName(), "TESTSOURCE");
    ASSERT_EQ(cps.sourceType, "File");
    /// Parser config
    ASSERT_EQ(cps.parserConfig.parserType, std::string("CSV"));
    ASSERT_EQ(cps.parserConfig.tupleDelimiter, std::string("\n"));
    ASSERT_EQ(cps.parserConfig.fieldDelimiter, std::string(","));
    /// Source config
    ASSERT_TRUE(cps.sourceConfig.contains("filePath"));
    ASSERT_EQ(cps.sourceConfig.at("filePath"), std::string("/dev/null"));

    const std::string dropPhysicalSourceStatement = "DROP PHYSICAL SOURCE 1";
    const auto statement3 = binder->parseAndBindSingle(dropPhysicalSourceStatement);
    ASSERT_TRUE(statement3.has_value());
    ASSERT_TRUE(std::holds_alternative<DropPhysicalSourceStatement>(*statement3));
    const auto& dps = std::get<DropPhysicalSourceStatement>(*statement3);
    ASSERT_EQ(dps.descriptor.getPhysicalSourceId().getRawValue(), 1);

    const std::string dropLogicalSourceStatement = "DROP LOGICAL SOURCE testSource";
    const auto statement4 = binder->parseAndBindSingle(dropLogicalSourceStatement);
    ASSERT_TRUE(statement4.has_value());
    ASSERT_TRUE(std::holds_alternative<DropLogicalSourceStatement>(*statement4));
    const auto& dls = std::get<DropLogicalSourceStatement>(*statement4);
    ASSERT_EQ(dls.source.getLogicalSourceName(), "TESTSOURCE");
}

TEST_F(StatementBinderTest, BindCreateBindSourceWithInvalidConfigs)
{
    Schema schema{};
    schema.addField("ATTRIBUTE1", DataTypeProvider::provideDataType(DataType::Type::UINT32));
    schema.addField("ATTRIBUTE2", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    auto logicalSourceOpt = sourceCatalog->addLogicalSource("TESTSOURCE", schema);
    ASSERT_TRUE(logicalSourceOpt.has_value());

    /// Misspelled CSV
    const std::string createPhysicalSourceStatement
        = R"(CREATE PHYSICAL SOURCE FOR testSource TYPE File SET ('/dev/null' AS `SOURCE`.FILE_PATH, 'CVS' AS PARSER.`TYPE`))";
    const auto statement = binder->parseAndBindSingle(createPhysicalSourceStatement);
    ASSERT_FALSE(statement.has_value());

    const std::string createPhysicalSourceStatement2
        = R"(CREATE PHYSICAL SOURCE FOR testSource TYPE File SET ('/dev/null' AS `SOURCE`.FILE_PATH, 'INVALID PARSER' AS PARSER.`TYPE`))";
    const auto statement2 = binder->parseAndBindSingle(createPhysicalSourceStatement2);
    ASSERT_FALSE(statement2.has_value());

    /// Invalid logical source
    const std::string createPhysicalSourceStatement3
        = "CREATE PHYSICAL SOURCE FOR invalidSource TYPE File SET ('/dev/null' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`)";
    const auto statement3 = binder->parseAndBindSingle(createPhysicalSourceStatement3);
    ASSERT_FALSE(statement3.has_value());
}

TEST_F(StatementBinderTest, BindCreateSink)
{
    const std::string createSinkStatement = "CREATE SINK testSink (attribute1 UINT32, attribute2 VARSIZED) TYPE File SET ('/dev/null' AS "
                                            "`SINK`.FILE_PATH, 'CSV' AS `SINK`.INPUT_FORMAT)";
    const auto statement = binder->parseAndBindSingle(createSinkStatement);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<CreateSinkStatement>(*statement));

    const auto& cs = std::get<CreateSinkStatement>(*statement);
    ASSERT_EQ(cs.name, "TESTSINK");
    ASSERT_EQ(cs.sinkType, "File");
    ASSERT_TRUE(cs.schema.contains("ATTRIBUTE1"));
    ASSERT_TRUE(cs.schema.contains("ATTRIBUTE2"));
    ASSERT_TRUE(cs.sinkConfig.contains("filePath"));
    ASSERT_TRUE(cs.sinkConfig.contains("inputFormat"));
    ASSERT_EQ(cs.sinkConfig.at("filePath"), std::string("/dev/null"));
    ASSERT_EQ(cs.sinkConfig.at("inputFormat"), std::string("CSV"));

    /// Register the sink in the catalog so DROP SINK can resolve the descriptor
    ASSERT_TRUE(sinkCatalog->addSinkDescriptor(cs.name, cs.schema, cs.sinkType, cs.sinkConfig).has_value());

    const std::string dropSinkStatement = "DROP SINK testSink";
    const auto statement2 = binder->parseAndBindSingle(dropSinkStatement);
    ASSERT_TRUE(statement2.has_value());
    ASSERT_TRUE(std::holds_alternative<DropSinkStatement>(*statement2));
}

/// TODO #764 Remove test when we have proper schema inference and don't require matching source names in sinks anymore
TEST_F(StatementBinderTest, BindCreateSinkWithQualifiedColumns)
{
    const std::string createSinkStatement = "CREATE SINK testSink (testSource.attribute1 UINT32, testSource.attribute2 VARSIZED) TYPE File "
                                            "SET ('/dev/null' AS `SINK`.FILE_PATH, 'CSV' AS `SINK`.INPUT_FORMAT)";
    const auto statement = binder->parseAndBindSingle(createSinkStatement);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<CreateSinkStatement>(*statement));

    const auto& cs = std::get<CreateSinkStatement>(*statement);
    Schema expectedSchema{};
    expectedSchema.addField("TESTSOURCE$ATTRIBUTE1", DataTypeProvider::provideDataType(DataType::Type::UINT32));
    expectedSchema.addField("TESTSOURCE$ATTRIBUTE2", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    ASSERT_EQ(cs.schema, expectedSchema);
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
        ASSERT_TRUE(std::holds_alternative<CreateLogicalSourceStatement>(*statement));
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

    const auto filteredQuotedStatementExp = binder->parseAndBindSingle(filteredQuotedLogicalQueryString);
    ASSERT_TRUE(filteredQuotedStatementExp.has_value());
    ASSERT_TRUE(std::holds_alternative<ShowLogicalSourcesStatement>(*filteredQuotedStatementExp));
    const auto [name2, format2] = std::get<ShowLogicalSourcesStatement>(*filteredQuotedStatementExp);
    ASSERT_TRUE(name2.has_value());
    ASSERT_EQ(*name2, "TESTSOURCE1");
    ASSERT_TRUE(format2 == std::nullopt);

    const auto invalidFormatStatementExp = binder->parseAndBindSingle(invalidFormatQueryString);
    ASSERT_FALSE(invalidFormatStatementExp.has_value());

    const auto formatInInvalidPositionStatementExp = binder->parseAndBindSingle(formatInInvalidPositionString);
    ASSERT_FALSE(formatInInvalidPositionStatementExp.has_value());
}

TEST_F(StatementBinderTest, ShowPhysicalSources)
{
    /// Seed logical sources so CREATE PHYSICAL SOURCE binds successfully
    {
        Schema s1{};
        s1.addField("ATTRIBUTE1", DataTypeProvider::provideDataType(DataType::Type::UINT32));
        s1.addField("ATTRIBUTE2", DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
        ASSERT_TRUE(sourceCatalog->addLogicalSource("TESTSOURCE1", s1).has_value());

        Schema s2{};
        s2.addField("ATTRIBUTE1", DataTypeProvider::provideDataType(DataType::Type::UINT32));
        s2.addField("ATTRIBUTE2", DataTypeProvider::provideDataType(DataType::Type::INT32));
        ASSERT_TRUE(sourceCatalog->addLogicalSource("TESTSOURCE2", s2).has_value());
    }

    const std::vector<std::string_view> createStmts = {
        "CREATE LOGICAL SOURCE testSource1 (attribute1 UINT32, attribute2 VARSIZED)",
        "CREATE LOGICAL SOURCE testSource2 (attribute1 UINT32, attribute2 INT32)",
        "CREATE PHYSICAL SOURCE FOR testSource1 TYPE File SET (200 as `SOURCE`.NUMBER_OF_BUFFERS_IN_LOCAL_POOL, '/dev/null' AS "
        "`SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER)",
        "CREATE PHYSICAL SOURCE FOR testSource2 TYPE File SET (-1 as `SOURCE`.NUMBER_OF_BUFFERS_IN_LOCAL_POOL, '/dev/random' AS "
        "`SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER)",
        "CREATE PHYSICAL SOURCE FOR testSource2 TYPE File SET (-1 as `SOURCE`.NUMBER_OF_BUFFERS_IN_LOCAL_POOL, '/dev/ones' AS "
        "`SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER)",
    };

    for (const auto& sql : createStmts)
    {
        const auto bound = binder->parseAndBindSingle(sql);
        ASSERT_TRUE(bound.has_value());

        std::visit(
            [&](const auto& st)
            {
                using T = std::decay_t<decltype(st)>;
                if constexpr (std::is_same_v<T, CreateLogicalSourceStatement>)
                {
                    ASSERT_TRUE(st.schema.contains("ATTRIBUTE1"));
                    ASSERT_TRUE(st.schema.contains("ATTRIBUTE2"));
                    /// Logical sources already seeded, just verify they match
                    ASSERT_TRUE(st.name == "TESTSOURCE1" || st.name == "TESTSOURCE2");
                }
                else if constexpr (std::is_same_v<T, CreatePhysicalSourceStatement>)
                {
                    ASSERT_TRUE(
                        st.attachedTo.getLogicalSourceName() == "TESTSOURCE1" || st.attachedTo.getLogicalSourceName() == "TESTSOURCE2");
                    ASSERT_EQ(st.sourceType, "File");
                    ASSERT_EQ(st.parserConfig.parserType, std::string("CSV"));
                    ASSERT_EQ(st.parserConfig.tupleDelimiter, std::string("\n"));
                    ASSERT_EQ(st.parserConfig.fieldDelimiter, std::string(","));
                    ASSERT_TRUE(st.sourceConfig.contains("filePath"));
                    /// Add to catalog so follow-up DROP/SHOW tests could rely on it if needed
                    ASSERT_TRUE(
                        sourceCatalog->addPhysicalSource(st.attachedTo, st.sourceType, st.sourceConfig, st.parserConfig).has_value());
                }
                else
                {
                    FAIL() << "Unexpected statement type in createStmts";
                }
            },
            bound.value());
    }

    const std::string_view allPhysicalSourcesStatementString = "SHOW PHYSICAL SOURCES";
    const std::string_view filteredPhysicalSourcesStatementString = "SHOW PHYSICAL SOURCES WHERE ID = 2 FORMAT JSON";
    const std::string_view physicalSourceForLogicalSourceStatementString = "SHOW PHYSICAL SOURCES FOR testSourCe1 FORMAT TEXT";
    const std::string_view physicalSourceForLogicalSourceStatementFilteredString = "SHOW PHYSICAL SOURCES FOR testSource2 WHERE ID = 3";
    const std::string_view invalidFilterAttr = "SHOW PHYSICAL SOURCES WHERE NAME = 'X'";
    const std::string_view invalidIdType = "SHOW PHYSICAL SOURCES WHERE ID = 'X'";
    const std::string_view unknownLogicalSource = "SHOW PHYSICAL SOURCES FOR unknown";

    const auto allPhysicalSourcesStatementExp = binder->parseAndBindSingle(allPhysicalSourcesStatementString);
    ASSERT_TRUE(allPhysicalSourcesStatementExp.has_value());
    ASSERT_TRUE(std::holds_alternative<ShowPhysicalSourcesStatement>(*allPhysicalSourcesStatementExp));
    auto showAllPhysicalSources = std::get<ShowPhysicalSourcesStatement>(*allPhysicalSourcesStatementExp);
    const auto [logicalSource, id, format] = showAllPhysicalSources;
    ASSERT_FALSE(logicalSource.has_value());
    ASSERT_FALSE(id.has_value());
    ASSERT_TRUE(format == std::nullopt);

    const auto filteredPhysicalSourcesStatementExp = binder->parseAndBindSingle(filteredPhysicalSourcesStatementString);
    ASSERT_TRUE(filteredPhysicalSourcesStatementExp.has_value());
    ASSERT_TRUE(std::holds_alternative<ShowPhysicalSourcesStatement>(*filteredPhysicalSourcesStatementExp));
    auto showFilteredPhysicalSources = std::get<ShowPhysicalSourcesStatement>(*filteredPhysicalSourcesStatementExp);
    const auto [logicalSource2, id2, format2] = showFilteredPhysicalSources;
    ASSERT_FALSE(logicalSource2.has_value());
    ASSERT_TRUE(id2.has_value());
    ASSERT_TRUE(*id2 == 2);
    ASSERT_TRUE(format2 == StatementOutputFormat::JSON);

    const auto physicalSourceForLogicalSourceStatementExp = binder->parseAndBindSingle(physicalSourceForLogicalSourceStatementString);
    ASSERT_TRUE(physicalSourceForLogicalSourceStatementExp.has_value());
    ASSERT_TRUE(std::holds_alternative<ShowPhysicalSourcesStatement>(*physicalSourceForLogicalSourceStatementExp));
    auto showPhysicalSourceForLogicalSource = std::get<ShowPhysicalSourcesStatement>(*physicalSourceForLogicalSourceStatementExp);
    const auto [logicalSource3, id3, format3] = showPhysicalSourceForLogicalSource;
    ASSERT_TRUE(logicalSource3.has_value());
    ASSERT_EQ(logicalSource3->getLogicalSourceName(), "TESTSOURCE1");
    ASSERT_FALSE(id3.has_value());
    ASSERT_TRUE(format3 == StatementOutputFormat::TEXT);

    const auto physicalSourceForLogicalSourceStatementFilteredExp
        = binder->parseAndBindSingle(physicalSourceForLogicalSourceStatementFilteredString);
    ASSERT_TRUE(physicalSourceForLogicalSourceStatementFilteredExp.has_value());
    ASSERT_TRUE(std::holds_alternative<ShowPhysicalSourcesStatement>(*physicalSourceForLogicalSourceStatementFilteredExp));
    auto showPhysicalSourceForLogicalSourceFiltered
        = std::get<ShowPhysicalSourcesStatement>(*physicalSourceForLogicalSourceStatementFilteredExp);
    const auto [logicalSource4, id4, format4] = showPhysicalSourceForLogicalSourceFiltered;
    ASSERT_TRUE(logicalSource4.has_value());
    ASSERT_EQ(logicalSource4->getLogicalSourceName(), "TESTSOURCE2");
    ASSERT_TRUE(id4.has_value());
    ASSERT_TRUE(*id4 == 3);
    ASSERT_TRUE(format4 == std::nullopt);

    /// Invalid filter attribute should fail to bind
    const auto invalidFilterAttrExp = binder->parseAndBindSingle(invalidFilterAttr);
    ASSERT_FALSE(invalidFilterAttrExp.has_value());

    /// Invalid ID type should fail to bind
    const auto invalidIdTypeExp = binder->parseAndBindSingle(invalidIdType);
    ASSERT_FALSE(invalidIdTypeExp.has_value());

    /// Unknown logical source should fail to bind
    const auto unknownLogicalSourceExp = binder->parseAndBindSingle(unknownLogicalSource);
    ASSERT_FALSE(unknownLogicalSourceExp.has_value());
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
}

///NOLINTEND(bugprone-unchecked-optional-access)
}
}
