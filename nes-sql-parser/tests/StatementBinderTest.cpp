

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
#include <Common/DataTypes/DataTypeProvider.hpp>
#include "API/Schema.hpp"
#include "Identifiers/Identifiers.hpp"
#include "Identifiers/NESStrongType.hpp"
#include "Sources/SourceDescriptor.hpp"
#include "Sources/SourceValidationProvider.hpp"

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
    std::shared_ptr<NES::Catalogs::Source::SourceCatalog> sourceCatalog;
    std::shared_ptr<Binder::StatementBinder> binder;

    /* Will be called before a test is executed. */
    static void SetUpTestCase()
    {
        Logger::setupLogging("StatementBinderTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatementBinderTest test case.");
    }
    void SetUp() override
    {
        BaseUnitTest::SetUp();
        sourceCatalog = std::make_shared<NES::Catalogs::Source::SourceCatalog>();
        binder = std::make_shared<Binder::StatementBinder>(
            sourceCatalog, std::bind(AntlrSQLQueryParser::bindLogicalQueryPlan, std::placeholders::_1));
    }
};


TEST_F(StatementBinderTest, BindQuery)
{
    /// This test only checks that the input was detected as a query, for more detailed testing of the QueryBinder
    /// see the AntlrSQLQueryParserTest and the Systests
    /// In the future this will require setting up the source catalog correctly as well
    const std::string queryString = "SELECT a FROM inputStream WHERE b < UINT32(5) INTO outputStream";
    const auto statement = binder->parseAndBind(queryString);
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<QueryPlan>>(statement));
}

TEST_F(StatementBinderTest, BindCreateBindSource)
{
    const std::string createLogicalSourceStatement = "CREATE LOGICAL SOURCE testSource (attribute1 UINT32, attribute2 VARSIZED)";
    const auto statement1 = binder->parseAndBind(createLogicalSourceStatement);
    ASSERT_TRUE(std::holds_alternative<Binder::CreateLogicalSourceStatement>(statement1));
    const auto [actualSourceExp] = std::get<Binder::CreateLogicalSourceStatement>(statement1);
    ASSERT_TRUE(actualSourceExp.has_value());
    const auto& actualSource = actualSourceExp.value();
    Schema expectedSchema{};
    auto expectedColumns = std::vector<std::pair<std::string, std::shared_ptr<DataType>>>{};
    expectedSchema.addField("attribute1", DataTypeProvider::provideBasicType(BasicType::UINT32));
    expectedSchema.addField("attribute2", DataTypeProvider::provideDataType(LogicalType::VARSIZED));
    ASSERT_EQ(actualSource.getLogicalSourceName(), "testSource");
    ASSERT_EQ(*actualSource.getSchema(), expectedSchema);

    const std::string createPhysicalSourceStatement
        = R"(CREATE PHYSICAL SOURCE FOR testSource TYPE file SET ('LOCAL' as `SOURCE`.LOCATION, -1 as `SOURCE`.POOL_BUFFERS, '/dev/null' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER))";
    const auto statement2 = binder->parseAndBind(createPhysicalSourceStatement);
    const Sources::ParserConfig expectedParserConfig{.parserType = "CSV", .tupleDelimiter = "\\n", .fieldDelimiter = ","};
    std::unordered_map<std::string, std::string> unvalidatedConfig{{"type", "file"}, {"filePath", "/dev/null"}};
    const Configurations::DescriptorConfig::Config descriptorConfig = Sources::SourceValidationProvider::provide("file", std::move(unvalidatedConfig));

    ASSERT_TRUE(std::holds_alternative<Binder::CreatePhysicalSourceStatement>(statement2));
    const auto [created] = std::get<Binder::CreatePhysicalSourceStatement>(statement2);
    const auto physicalSource = created.value();
    ASSERT_EQ(physicalSource.getLogicalSource(), actualSource);
    ASSERT_EQ(physicalSource.getWorkerId(), INITIAL<WorkerId>);
    ASSERT_EQ(physicalSource.getParserConfig(), expectedParserConfig);
    ASSERT_EQ(physicalSource.getBuffersInLocalPool(), -1);
    ASSERT_EQ(physicalSource.getSourceType(), "file");
    ASSERT_EQ(physicalSource.getConfig(), descriptorConfig);
    ASSERT_EQ(physicalSource.getPhysicalSourceId(), 1);
}
