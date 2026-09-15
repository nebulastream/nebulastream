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

#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <CompilationCache.hpp>
#include <OptimizedLogicalPlanSignatureUtil.hpp>
#include <Pipeline.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <QueryId.hpp>
#include <SinkPhysicalOperator.hpp>
#include <options.hpp>

namespace NES
{
namespace
{
class OptimizedLogicalPlanSignatureUtilTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("OptimizedLogicalPlanSignatureUtilTest.log", LogLevel::LOG_DEBUG); }
};

LogicalPlan createPlan(
    const DataType dataType, std::string sourcePath = "/dev/null", std::string sinkPath = "/dev/null", std::string fieldDelimiter = ",")
{
    const auto sourceName = Identifier::parse("SOURCE");
    const auto sinkName = Identifier::parse("SINK");
    const auto schema
        = Schema<UnqualifiedUnboundField, Ordered>{std::vector{UnqualifiedUnboundField{Identifier::parse("VALUE"), dataType}}};

    SourceCatalog sourceCatalog;
    const auto logicalSource = sourceCatalog.addLogicalSource(sourceName, schema).value();
    const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("FILE_PATH"), std::move(sourcePath)}};
    const std::unordered_map<Identifier, std::string> formatterConfig{
        {Identifier::parse("TYPE"), "CSV"},
        {Identifier::parse("FIELD_DELIMITER"), std::move(fieldDelimiter)},
        {Identifier::parse("TUPLE_DELIMITER"), "\n"}};
    const auto sourceDescriptor
        = sourceCatalog.addPhysicalSource(logicalSource, Identifier::parse("file"), Host{"localhost"}, sourceConfig, formatterConfig)
              .value();

    SinkCatalog sinkCatalog;
    const std::unordered_map<Identifier, std::string> sinkConfig{
        {Identifier::parse("FILE_PATH"), std::move(sinkPath)}, {Identifier::parse("OUTPUT_FORMAT"), "CSV"}};
    const auto sinkDescriptor
        = sinkCatalog.addSinkDescriptor(sinkName, schema, Identifier::parse("file"), Host{"localhost"}, sinkConfig, {}).value();

    auto source = SourceDescriptorLogicalOperator::create(sourceDescriptor);
    auto sink = SinkLogicalOperator::create(std::move(source), sinkDescriptor);
    return LogicalPlan{
        QueryId::create(LocalQueryId{LocalQueryId::INVALID}, DistributedQueryId{DistributedQueryId::INVALID}), {std::move(sink)}};
}

std::string createSignature(const LogicalPlan& plan, const QueryExecutionConfiguration& configuration = {})
{
    return OptimizedLogicalPlanSignatureUtil::create(plan, configuration);
}
}

TEST_F(OptimizedLogicalPlanSignatureUtilTest, GeneratedOperatorIdsDoNotAffectSignatures)
{
    const auto dataType = DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto firstPlan = createPlan(dataType);
    const auto secondPlan = createPlan(dataType);

    ASSERT_NE(firstPlan.getRootOperators().front().getId(), secondPlan.getRootOperators().front().getId());
    EXPECT_EQ(createSignature(firstPlan), createSignature(secondPlan));
}

TEST_F(OptimizedLogicalPlanSignatureUtilTest, RepeatedEquivalentPlansProduceIdenticalSignatures)
{
    const auto plan = createPlan(DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE});

    EXPECT_EQ(createSignature(plan), createSignature(plan));
}

TEST_F(OptimizedLogicalPlanSignatureUtilTest, RuntimeConnectorLocationsDoNotAffectSignatures)
{
    const auto dataType = DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto firstPlan = createPlan(dataType, "/tmp/source-a", "/tmp/sink-a");
    const auto secondPlan = createPlan(dataType, "/tmp/source-b", "/tmp/sink-b");

    EXPECT_EQ(createSignature(firstPlan), createSignature(secondPlan));
}

TEST_F(OptimizedLogicalPlanSignatureUtilTest, SchemaNullabilityAffectsSignatures)
{
    const auto requiredPlan = createPlan(DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE});
    const auto nullablePlan = createPlan(DataType{DataType::Type::UINT64, DataType::NULLABLE::IS_NULLABLE});

    EXPECT_NE(createSignature(requiredPlan), createSignature(nullablePlan));
}

TEST_F(OptimizedLogicalPlanSignatureUtilTest, FormatterConfigurationAffectsSignatures)
{
    const auto dataType = DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto commaDelimitedPlan = createPlan(dataType, "/dev/null", "/dev/null", ",");
    const auto semicolonDelimitedPlan = createPlan(dataType, "/dev/null", "/dev/null", ";");

    EXPECT_NE(createSignature(commaDelimitedPlan), createSignature(semicolonDelimitedPlan));
}

TEST_F(OptimizedLogicalPlanSignatureUtilTest, QueryExecutionConfigurationAffectsSignatures)
{
    const auto plan = createPlan(DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE});
    const QueryExecutionConfiguration defaultConfiguration;
    QueryExecutionConfiguration changedConfiguration;
    changedConfiguration.numberOfPartitions = DEFAULT_NUMBER_OF_PARTITIONS_DATASTRUCTURES * 2;

    EXPECT_NE(createSignature(plan, defaultConfiguration), createSignature(plan, changedConfiguration));
}

TEST_F(OptimizedLogicalPlanSignatureUtilTest, DisabledCacheSkipsSignatureGeneration)
{
    const LogicalPlan planWithoutInferredSchema{QueryId::invalid(), {UnionLogicalOperator::create()}};
    const QueryExecutionConfiguration configuration;
    const auto cacheDir = std::filesystem::temp_directory_path().string();

    for (const auto& settings : std::vector<QueryCompilation::CompilationCache::Settings>{{false, cacheDir}, {true, {}}})
    {
        SCOPED_TRACE(settings.enabled ? "empty cache directory" : "disabled cache");
        QueryCompilation::CompilationCache cache(settings);
        ASSERT_FALSE(cache.isEnabled());
        EXPECT_NO_THROW(cache.prepareForQuery(planWithoutInferredSchema, configuration));
    }
}

#ifdef __linux__
TEST_F(OptimizedLogicalPlanSignatureUtilTest, EnabledCacheConfiguresSemanticQueryKey)
{
    const auto plan = createPlan(DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE});
    const auto sink = plan.getRootOperators().front().getAs<SinkLogicalOperator>();
    const auto pipeline = std::make_shared<Pipeline>(SinkPhysicalOperator(sink->getSinkDescriptor().value()));
    QueryExecutionConfiguration configuration;
    configuration.numberOfPartitions = DEFAULT_NUMBER_OF_PARTITIONS_DATASTRUCTURES * 2;
    const auto cacheDir = std::filesystem::temp_directory_path().string();
    QueryCompilation::CompilationCache cache({true, cacheDir});
    ASSERT_TRUE(cache.isEnabled());

    cache.prepareForQuery(plan, configuration);
    nautilus::engine::EngineOptions options;
    cache.configureEngineOptionsForPipeline(options, pipeline);

    EXPECT_EQ(options.getOptionOrDefault("engine.Blob.CacheDir", std::string{}), cacheDir);
    const auto cacheKey = options.getOptionOrDefault("engine.Blob.CacheKey", std::string{});
    EXPECT_TRUE(cacheKey.starts_with("nes:auto|binary="));
    const auto queryKeyStart = cacheKey.find(":q=");
    ASSERT_NE(queryKeyStart, std::string::npos);
    EXPECT_EQ(cacheKey.substr(queryKeyStart), ":q=" + createSignature(plan, configuration) + ":o=0:h=0[]");
}
#endif
}
