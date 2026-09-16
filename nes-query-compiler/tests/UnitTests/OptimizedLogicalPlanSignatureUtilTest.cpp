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

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <gtest/gtest.h>
#include <nautilus/CompilationStatistics.hpp>
#include <nautilus/Engine.hpp>
#include <nautilus/val.hpp>
#include <yaml-cpp/yaml.h>
#include <scope_guard.hpp>

#include <Configuration/WorkerConfiguration.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
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
#include <CompilationCacheConfiguration.hpp>
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

LogicalOperator selectValue(const LogicalOperator& child, std::string value)
{
    const auto field = child.getOutputSchema()[Identifier::parse("VALUE")].value();
    return SelectionLogicalOperator::create(
        child,
        EqualsLogicalFunction{FieldAccessLogicalFunction{field}, ConstantValueLogicalFunction{field.getDataType(), std::move(value)}});
}

LogicalPlan createFilteredPlan(const DataType type, const std::vector<std::string>& values)
{
    const auto plan = createPlan(type);
    const auto sink = plan.getRootOperators().front().getAs<SinkLogicalOperator>();
    auto child = sink.getChildren().front();
    for (const auto& value : values)
    {
        child = selectValue(child, value);
    }
    return LogicalPlan{QueryId::invalid(), {SinkLogicalOperator::create(child, sink->getSinkDescriptor().value())}};
}

LogicalPlan createUnionPlan(const bool shareSource, const bool reverseChildren = false)
{
    const auto type = DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto plan = createPlan(type);
    const auto sink = plan.getRootOperators().front().getAs<SinkLogicalOperator>();
    const auto source = sink.getChildren().front();
    const auto rightSource = shareSource ? source : createPlan(type).getRootOperators().front().getChildren().front();
    auto left = selectValue(source, "1");
    auto right = selectValue(rightSource, "2");
    auto children = reverseChildren ? std::vector{right, left} : std::vector{left, right};
    const auto merged = UnionLogicalOperator::create(std::move(children));
    return LogicalPlan{QueryId::invalid(), {SinkLogicalOperator::create(merged, sink->getSinkDescriptor().value())}};
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

TEST_F(OptimizedLogicalPlanSignatureUtilTest, CacheConfigurationUsesNestedOptions)
{
    WorkerConfiguration worker;
    EXPECT_FALSE(worker.compilationCache.enabled.getValue());
    EXPECT_EQ(worker.compilationCache.cacheDir.getValue(), "/tmp/nes-compilation-cache");
    EXPECT_FALSE(worker.compilationCache.isExplicitlySet());

    worker.overwriteConfigWithYAMLNode(YAML::Load("compilation_cache:\n  enabled: true\n  cache_dir: /tmp/nes-cache-config-test\n"));
    EXPECT_TRUE(worker.compilationCache.enabled.getValue());
    EXPECT_EQ(worker.compilationCache.cacheDir.getValue(), "/tmp/nes-cache-config-test");

    WorkerConfiguration overrides;
    overrides.overwriteConfigWithCommandLineInput({{"compilation_cache.enabled", "false"}});
    worker.applyExplicitlySetFrom(overrides);
    EXPECT_FALSE(worker.compilationCache.enabled.getValue());
    EXPECT_EQ(worker.compilationCache.cacheDir.getValue(), "/tmp/nes-cache-config-test");

    worker.clear();
    EXPECT_FALSE(worker.compilationCache.enabled.getValue());
    EXPECT_EQ(worker.compilationCache.cacheDir.getValue(), "/tmp/nes-compilation-cache");
    EXPECT_FALSE(worker.compilationCache.isExplicitlySet());
}

TEST_F(OptimizedLogicalPlanSignatureUtilTest, DisabledCacheSkipsSignatureGeneration)
{
    const LogicalPlan planWithoutInferredSchema{QueryId::invalid(), {UnionLogicalOperator::create()}};
    const QueryExecutionConfiguration configuration;
    const auto cacheDir = std::filesystem::temp_directory_path().string();

    CompilationCacheConfiguration disabled;
    disabled.cacheDir = cacheDir;
    CompilationCacheConfiguration emptyDirectory;
    emptyDirectory.enabled = true;
    emptyDirectory.cacheDir = std::string{};
    for (const auto& cacheConfiguration : {disabled, emptyDirectory})
    {
        SCOPED_TRACE(cacheConfiguration.enabled.getValue() ? "empty cache directory" : "disabled cache");
        QueryCompilation::CompilationCache cache(cacheConfiguration);
        ASSERT_FALSE(cache.isEnabled());
        EXPECT_NO_THROW(cache.prepareForQuery(planWithoutInferredSchema, configuration));
    }
}

TEST_F(OptimizedLogicalPlanSignatureUtilTest, NestedBoundFieldsIgnoreGeneratedIds)
{
    const auto type = DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    EXPECT_EQ(createSignature(createFilteredPlan(type, {"1", "2"})), createSignature(createFilteredPlan(type, {"1", "2"})));
    EXPECT_NE(createSignature(createFilteredPlan(type, {"1", "2"})), createSignature(createFilteredPlan(type, {"1", "3"})));
}

TEST_F(OptimizedLogicalPlanSignatureUtilTest, LiteralBytesRemainPartOfIdentity)
{
    const auto type = DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE};
    const std::vector<std::string> values{"aaaa", "bbbb", std::string("a\0b", 3), std::string("a\0c", 3), "Grüße🌍"};
    std::vector<std::string> signatures;
    for (const auto& value : values)
    {
        const auto signature = createSignature(createFilteredPlan(type, {value}));
        EXPECT_EQ(signature, createSignature(createFilteredPlan(type, {value})));
        for (const auto& previous : signatures)
        {
            EXPECT_NE(signature, previous);
        }
        signatures.push_back(signature);
    }
}

TEST_F(OptimizedLogicalPlanSignatureUtilTest, SharedSubgraphsAndChildOrderArePreserved)
{
    const auto shared = createSignature(createUnionPlan(true));
    EXPECT_EQ(shared, createSignature(createUnionPlan(true)));
    EXPECT_NE(shared, createSignature(createUnionPlan(false)));
    EXPECT_NE(shared, createSignature(createUnionPlan(true, true)));
}

TEST_F(OptimizedLogicalPlanSignatureUtilTest, IngestionWatermarkIdentityIgnoresGeneratedId)
{
    const auto type = DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto makePlan = [&]
    {
        const auto plan = createPlan(type);
        const auto sink = plan.getRootOperators().front().getAs<SinkLogicalOperator>();
        const auto watermark = IngestionTimeWatermarkAssignerLogicalOperator::create(sink.getChildren().front());
        return LogicalPlan{QueryId::invalid(), {SinkLogicalOperator::create(watermark, sink->getSinkDescriptor().value())}};
    };
    EXPECT_EQ(createSignature(makePlan()), createSignature(makePlan()));
    EXPECT_NE(createSignature(makePlan()), createSignature(createPlan(type)));
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
    CompilationCacheConfiguration cacheConfiguration;
    cacheConfiguration.enabled = true;
    cacheConfiguration.cacheDir = cacheDir;
    QueryCompilation::CompilationCache cache(cacheConfiguration);
    ASSERT_TRUE(cache.isEnabled());

    cache.prepareForQuery(plan, configuration);
    nautilus::engine::EngineOptions options;
    cache.configureEngineOptionsForPipeline(options, *pipeline);

    EXPECT_EQ(options.getOptionOrDefault("engine.Blob.CacheDir", std::string{}), cacheDir);
    const auto cacheKey = options.getOptionOrDefault("engine.Blob.CacheKey", std::string{});
    EXPECT_TRUE(cacheKey.starts_with("nes:auto|binary="));
    const auto queryKeyStart = cacheKey.find(":q=");
    ASSERT_NE(queryKeyStart, std::string::npos);
    EXPECT_EQ(cacheKey.substr(queryKeyStart), ":q=" + createSignature(plan, configuration) + ":o=0:h=0[]");
}

TEST_F(OptimizedLogicalPlanSignatureUtilTest, PipelineOrdinalsResetOnlyWhenPreparingAQuery)
{
    const auto type = DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    const auto plan = createPlan(type);
    const auto sink = plan.getRootOperators().front().getAs<SinkLogicalOperator>();
    const auto first = std::make_shared<Pipeline>(SinkPhysicalOperator(sink->getSinkDescriptor().value()));
    const auto second = std::make_shared<Pipeline>(SinkPhysicalOperator(sink->getSinkDescriptor().value()));
    const auto cacheDir = std::filesystem::temp_directory_path().string();
    CompilationCacheConfiguration cacheConfiguration;
    cacheConfiguration.enabled = true;
    cacheConfiguration.cacheDir = cacheDir;
    QueryCompilation::CompilationCache cache(cacheConfiguration);
    ASSERT_TRUE(cache.isEnabled());
    const auto keyFor = [&](const Pipeline& pipeline)
    {
        nautilus::engine::EngineOptions options;
        cache.configureEngineOptionsForPipeline(options, pipeline);
        return options.getOptionOrDefault("engine.Blob.CacheKey", std::string{});
    };

    cache.prepareForQuery(plan, {});
    const auto firstKey = keyFor(*first);
    const auto secondKey = keyFor(*second);
    EXPECT_NE(firstKey, secondKey);
    EXPECT_EQ(keyFor(*first), firstKey);
    EXPECT_EQ(keyFor(*second), secondKey);

    cache.prepareForQuery(createPlan(type), {});
    EXPECT_EQ(keyFor(*second), firstKey);
    EXPECT_EQ(keyFor(*first), secondKey);

    QueryExecutionConfiguration changed;
    changed.numberOfPartitions = DEFAULT_NUMBER_OF_PARTITIONS_DATASTRUCTURES * 2;
    cache.prepareForQuery(plan, changed);
    EXPECT_NE(keyFor(*first), firstKey);
    EXPECT_TRUE(keyFor(*first).ends_with(":o=0:h=0[]"));
}

TEST_F(OptimizedLogicalPlanSignatureUtilTest, WorkerCountOptionSeparatesNativeArtifacts)
{
    const auto cache = std::filesystem::temp_directory_path()
        / ("nes-worker-count-cache-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    ASSERT_TRUE(std::filesystem::create_directory(cache));
    SCOPE_EXIT
    {
        std::error_code error;
        std::filesystem::remove_all(cache, error);
    };

    nautilus::engine::Options options;
    options.setOption("engine.backend", "mlir");
    options.setOption("engine.compiler", "legacy");
    options.setOption("engine.Blob.CacheDir", cache.string());
    options.setOption("engine.Blob.CacheKey", "nes-worker-count-option-test");
    nautilus::engine::NautilusEngine engine(options);
    std::unordered_map<uint64_t, std::string> keys;
    for (const uint64_t workers : {uint64_t{1}, uint64_t{4}, (uint64_t{1} << 32) + 1, uint64_t{1}, uint64_t{4}, (uint64_t{1} << 32) + 1})
    {
        SCOPED_TRACE(workers);
        const bool warm = keys.contains(workers);
        auto module = engine.createModule();
        module.setOption("nes.numberOfWorkerThreads", std::to_string(workers));
        EXPECT_EQ(module.getOptions().getOptionOrDefault("engine.Blob.CacheKey", std::string{}), "nes-worker-count-option-test");
        uint64_t traces = 0;
        module.registerFunction<nautilus::val<uint64_t>()>(
            "count",
            [&traces, workers]() -> nautilus::val<uint64_t>
            {
                ++traces;
                return workers;
            });
        auto compiled = module.compile();
        EXPECT_EQ(compiled.getFunction<uint64_t()>("count")(), workers);
        const auto statistics = compiled.getStatistics();
        ASSERT_NE(statistics, nullptr);
        const auto* outcome = statistics->find("cache.object");
        const auto* traced = statistics->find("cache.tracingRan");
        const auto* key = statistics->find("cache.key");
        ASSERT_NE(outcome, nullptr);
        ASSERT_NE(traced, nullptr);
        ASSERT_NE(key, nullptr);
        EXPECT_EQ(std::get<std::string>(*outcome), warm ? "hit" : "written");
        EXPECT_EQ(std::get<int64_t>(*traced), warm ? 0 : 1);
        EXPECT_EQ(traces == 0, warm);
        if (warm)
        {
            EXPECT_EQ(std::get<std::string>(*key), keys.at(workers));
        }
        else
        {
            for (const auto& [otherWorkers, otherKey] : keys)
            {
                EXPECT_NE(std::get<std::string>(*key), otherKey);
            }
            keys.emplace(workers, std::get<std::string>(*key));
        }
    }
}
#endif
}
