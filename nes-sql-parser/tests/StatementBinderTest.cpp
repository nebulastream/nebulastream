

#include <memory>
#include <string>

#include <MemoryLayout/RowLayout.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <StatementHandler.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/OperatorsUtil.hpp>

#include <SerializableOperator.pb.h>

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
using namespace NES;
class StatementBinderTest : public Testing::BaseUnitTest
{
public:
    std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog{};
    std::shared_ptr<StatementBinder> binder{};
    std::shared_ptr<SourceStatementHandler> sourceStatementHandler{};

    /* Will be called before a test is executed. */
    static void SetUpTestCase()
    {
        Logger::setupLogging("StatementBinderTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatementBinderTest test case.");
    }
    void SetUp() override
    {
        BaseUnitTest::SetUp();
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        binder
            = std::make_shared<StatementBinder>(sourceCatalog, std::bind(AntlrSQLQueryParser::bindLogicalQueryPlan, std::placeholders::_1));
        sourceStatementHandler = std::make_shared<SourceStatementHandler>(sourceCatalog);
    }
};


TEST_F(StatementBinderTest, BindQuery)
{
    /// This test only checks that the input was detected as a query, for more detailed testing of the QueryBinder
    /// see the AntlrSQLQueryParserTest and the Systests
    /// In the future this will require setting up the source catalog correctly as well
    const std::string queryString = "SELECT a FROM inputStream WHERE b < UINT32(5) INTO outputStream";
    const auto statement = binder->parseAndBind(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<QueryPlan>>(*statement));
}

TEST_F(StatementBinderTest, BindCreateBindSource)
{
    const std::string createLogicalSourceStatement = "CREATE LOGICAL SOURCE testSource (attribute1 UINT32, attribute2 VARSIZED)";
    const auto statement1 = binder->parseAndBind(createLogicalSourceStatement);
    ASSERT_TRUE(statement1.has_value());
    ASSERT_TRUE(std::holds_alternative<CreateLogicalSourceStatement>(*statement1));
    const auto createdSourceResult = sourceStatementHandler->apply(std::get<CreateLogicalSourceStatement>(*statement1));
    ASSERT_TRUE(createdSourceResult.has_value());
    const auto [actualSource] = createdSourceResult.value();
    Schema expectedSchema{};
    auto expectedColumns = std::vector<std::pair<std::string, std::shared_ptr<DataType>>>{};
    expectedSchema.addField("ATTRIBUTE1", DataTypeProvider::provideBasicType(BasicType::UINT32));
    expectedSchema.addField("ATTRIBUTE2", DataTypeProvider::provideDataType(LogicalType::VARSIZED));
    ASSERT_EQ(actualSource.getLogicalSourceName(), "testSource");
    ASSERT_EQ(*actualSource.getSchema(), expectedSchema);

    const std::string createPhysicalSourceStatement
        = R"(CREATE PHYSICAL SOURCE FOR testSource TYPE File SET ('LOCAL' as `SOURCE`.LOCATION, -1 as `SOURCE`.BUFFERS_IN_LOCAL_POOL, '/dev/null' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER))";
    const auto statement2 = binder->parseAndBind(createPhysicalSourceStatement);
    const Sources::ParserConfig expectedParserConfig{.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","};
    std::unordered_map<std::string, std::string> unvalidatedConfig{{"type", "File"}, {"filePath", "/dev/null"}};
    const Configurations::DescriptorConfig::Config descriptorConfig
        = Sources::SourceValidationProvider::provide("File", std::move(unvalidatedConfig));

    ASSERT_TRUE(statement2.has_value());
    ASSERT_TRUE(std::holds_alternative<CreatePhysicalSourceStatement>(*statement2));
    const auto physicalSourceResult = sourceStatementHandler->apply(std::get<CreatePhysicalSourceStatement>(*statement2));
    ASSERT_TRUE(physicalSourceResult.has_value());
    const auto [physicalSource] = physicalSourceResult.value();
    ASSERT_EQ(physicalSource.getLogicalSource(), actualSource);
    ASSERT_EQ(physicalSource.getWorkerId(), INITIAL<WorkerId>);
    ASSERT_EQ(physicalSource.getParserConfig(), expectedParserConfig);
    ASSERT_EQ(physicalSource.getBuffersInLocalPool(), -1);
    ASSERT_EQ(physicalSource.getSourceType(), "File");
    ASSERT_EQ(physicalSource.getConfig(), descriptorConfig);
    ASSERT_EQ(physicalSource.getPhysicalSourceId(), 0);

    const std::string dropPhysicalSourceStatement = "DROP PHYSICAL SOURCE 0";
    const auto statement3 = binder->parseAndBind(dropPhysicalSourceStatement);
    ASSERT_TRUE(statement3.has_value());
    ASSERT_TRUE(std::holds_alternative<DropPhysicalSourceStatement>(*statement3));
    const auto droppedResult = sourceStatementHandler->apply(std::get<DropPhysicalSourceStatement>(*statement3));
    ASSERT_TRUE(droppedResult.has_value());
    const auto [dropped] = droppedResult.value();
    ASSERT_EQ(dropped.getPhysicalSourceId(), 0);
    auto remainingPhysicalSources = sourceCatalog->getPhysicalSources(actualSource);
    ASSERT_TRUE(remainingPhysicalSources.has_value());
    ASSERT_EQ(remainingPhysicalSources.value().size(), 0);

    const std::string dropLogicalSourceStatement = "DROP LOGICAL SOURCE testSource";
    const auto statement4 = binder->parseAndBind(dropLogicalSourceStatement);
    ASSERT_TRUE(statement4.has_value());
    ASSERT_TRUE(std::holds_alternative<DropLogicalSourceStatement>(*statement4));
    const auto dropped2Result = sourceStatementHandler->apply(std::get<DropLogicalSourceStatement>(*statement4));
    ASSERT_TRUE(dropped2Result.has_value());
    const auto [dropped2] = dropped2Result.value();
    ASSERT_EQ(dropped2.getLogicalSourceName(), "testSource");
    auto remainingLogicalSources = sourceCatalog->getAllSources();
    ASSERT_EQ(remainingLogicalSources.size(), 0);
}


TEST_F(StatementBinderTest, BindCreateBindSourceWithInvalidConfigs)
{
    Schema schema{};
    auto expectedColumns = std::vector<std::pair<std::string, std::shared_ptr<DataType>>>{};
    schema.addField("ATTRIBUTE1", DataTypeProvider::provideBasicType(BasicType::UINT32));
    schema.addField("ATTRIBUTE2", DataTypeProvider::provideDataType(LogicalType::VARSIZED));
    const auto logicalSourceOpt = sourceCatalog->addLogicalSource("testSource", schema);
    ASSERT_TRUE(logicalSourceOpt.has_value());

    /// Misspelled location
    const std::string createPhysicalSourceStatement
        = R"(CREATE PHYSICAL SOURCE FOR testSource TYPE File SET ('LOCA' as `SOURCE`.LOCATION, '/dev/null' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`))";
    const auto statement = binder->parseAndBind(createPhysicalSourceStatement);
    ASSERT_FALSE(statement.has_value());

    /// TODO after #805 uncomment test for invalid parser type
    // const std::string createPhysicalSourceStatement2
    //     = R"(CREATE PHYSICAL SOURCE FOR testSource TYPE File SET ('LOCAL' as `SOURCE`.LOCATION, '/dev/null' AS `SOURCE`.FILE_PATH, 'INVALID PARSER' AS PARSER.`TYPE`))";
    // const auto statement2 = binder->parseAndBind(createPhysicalSourceStatement2);
    // ASSERT_FALSE(statement2.has_value());

    /// Invalid logical source
    const std::string createPhysicalSourceStatement3 = "CREATE PHYSICAL SOURCE FOR invalidSource TYPE File SET ('LOCAL' as "
                                                       "`SOURCE`.LOCATION, '/dev/null' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`)";
    const auto statement3 = binder->parseAndBind(createPhysicalSourceStatement3);
    ASSERT_FALSE(statement3.has_value());
}

TEST_F(StatementBinderTest, BindDropQuery)
{
    const std::string queryString = "DROP QUERY 12";
    const auto statement = binder->parseAndBind(queryString);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<DropQueryStatement>(*statement));
    ASSERT_EQ(std::get<DropQueryStatement>(*statement).id.getRawValue(), 12);

    const std::string queryString2 = "DROP QUERY -5";
    const auto statement2 = binder->parseAndBind(queryString2);
    ASSERT_FALSE(statement2.has_value());
    ASSERT_EQ(statement2.error().code(), ErrorCode::InvalidQuerySyntax);
}