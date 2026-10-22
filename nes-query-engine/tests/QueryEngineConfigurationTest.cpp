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

#include <cstdint>
#include <string>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/ConfigParsing.hpp>
#include <Configurations/Util.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <QueryEngineConfiguration.hpp>

namespace NES::Testing
{
namespace
{
/// Resolves a QueryEngineConfiguration from a single raw literal assigned to the given field.
auto resolveWith(const std::string& fullyQualifiedName, const ConfigLiteral& literal)
{
    return resolveConfiguration<QueryEngineConfiguration>(
        Schema<LiteralConfigValue, Ordered>{LiteralConfigValue{QualifiedIdentifier::parse(fullyQualifiedName), literal}});
}
}

class QueryEngineConfigurationTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("QueryEngineConfigurationTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup QueryEngineConfigurationTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(QueryEngineConfigurationTest, testConfigurationsDefault)
{
    const auto defaultConfig = defaultConfiguration<QueryEngineConfiguration>();
    EXPECT_EQ(defaultConfig.admissionQueueSize, 1000);
    EXPECT_EQ(defaultConfig.numberOfWorkerThreads, 4);
}

TEST_F(QueryEngineConfigurationTest, testConfigurationsValidInput)
{
    const auto config = resolveConfiguration<QueryEngineConfiguration>(Schema<LiteralConfigValue, Ordered>{
        LiteralConfigValue{QualifiedIdentifier::parse("query_engine.number_of_worker_threads"), parseConfigLiteral("2").value()},
        LiteralConfigValue{QualifiedIdentifier::parse("query_engine.admission_queue_size"), parseConfigLiteral("123").value()}});
    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config.value().numberOfWorkerThreads, 2);
    EXPECT_EQ(config.value().admissionQueueSize, 123);
}

TEST_F(QueryEngineConfigurationTest, testConfigurationsBadInputNonString)
{
    /// Non-numeric strings stay string literals and are rejected by the integer field factories
    EXPECT_FALSE(resolveWith("query_engine.admission_queue_size", parseConfigLiteral("XX").value()).has_value());
    EXPECT_FALSE(resolveWith("query_engine.number_of_worker_threads", parseConfigLiteral("XX").value()).has_value());

    /// Fractional literals are typed as double and are rejected by the integer field factories
    EXPECT_FALSE(resolveWith("query_engine.admission_queue_size", parseConfigLiteral("1.0").value()).has_value());
    EXPECT_FALSE(resolveWith("query_engine.number_of_worker_threads", parseConfigLiteral("1.5").value()).has_value());
}

TEST_F(QueryEngineConfigurationTest, testConfigurationsBadInputBadNumberOfThreads)
{
    EXPECT_FALSE(resolveWith("query_engine.number_of_worker_threads", ConfigLiteral{int64_t{0}}).has_value());
    EXPECT_FALSE(resolveWith("query_engine.number_of_worker_threads", ConfigLiteral{int64_t{-1}}).has_value());
    /// More worker threads than available CPUs are rejected
    EXPECT_FALSE(resolveWith("query_engine.number_of_worker_threads", ConfigLiteral{int64_t{20000}}).has_value());
}

}
