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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Sinks/FileSink.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <SystestConfiguration.hpp>
#include <SystestParser.hpp>
#include <SystestPreparation.hpp>
#include <SystestQueryModel.hpp>
#include <SystestResolver.hpp>
#include <WorkerConfig.hpp>

namespace NES::Systest
{
namespace
{

class TemporaryWorkspace
{
public:
    explicit TemporaryWorkspace(const std::string_view name)
    {
        static std::atomic<uint64_t> sequence = 0;
        root = std::filesystem::temp_directory_path()
            / (std::string{name} + "-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-"
               + std::to_string(sequence.fetch_add(1)));
        working = root / "working";
        testData = root / "test-data";
        std::filesystem::create_directories(working);
        std::filesystem::create_directories(testData);
    }

    ~TemporaryWorkspace()
    {
        std::error_code error;
        std::filesystem::remove_all(root, error);
    }

    TemporaryWorkspace(const TemporaryWorkspace&) = delete;
    TemporaryWorkspace& operator=(const TemporaryWorkspace&) = delete;

    std::filesystem::path root;
    std::filesystem::path working;
    std::filesystem::path testData;
};

ParsedTestFile
parseTestFile(const std::filesystem::path& root, const std::filesystem::path& relativeTestFile, const std::string_view contents)
{
    const auto file = root / relativeTestFile;
    std::filesystem::create_directories(file.parent_path());
    std::ofstream(file) << contents;

    SystestParser parser;
    if (!parser.loadFile(file, relativeTestFile))
    {
        throw TestException("Could not load test input {}", file);
    }
    return parser.parse();
}

SystestClusterConfiguration oneWorkerCluster()
{
    const auto host = Host{"localhost:8080"};
    return SystestClusterConfiguration{
        .workers = {WorkerConfig{
            .host = host, .dataAddress = "localhost:9090", .maxOperators = CapacityKind::Limited{1000}, .downstream = {}, .config = {}}},
        .allowSourcePlacement = {host},
        .allowSinkPlacement = {host}};
}

TestCaseId id(const std::filesystem::path& file, const uint64_t queryNumber, const uint32_t variant)
{
    return TestCaseId{
        .source = CaseKey{.relativeTestFile = file, .queryNumber = SystestQueryId{queryNumber}}, .configurationVariant = variant};
}

const std::shared_ptr<const PreparedAction>& preparedAction(const PreparedExecutionCatalog& catalog, const TestCaseId& testCase)
{
    return std::get<std::shared_ptr<const PreparedAction>>(catalog.at(testCase).prepared);
}

ResultSchema checksumSchema()
{
    return ResultSchema{std::vector{
        UnqualifiedUnboundField{Identifier::parse("COUNT"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        UnqualifiedUnboundField{Identifier::parse("CHECKSUM"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}}};
}

std::string readFile(const std::filesystem::path& file)
{
    std::ifstream input(file);
    return std::string{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
}

std::filesystem::path physicalSourceFile(const PreparedEnvironmentContext& context, const std::string_view sourceName)
{
    const auto logicalSource = context.sourceCatalog()->getLogicalSource(Identifier::parse(std::string{sourceName}));
    if (!logicalSource)
    {
        throw TestException("Missing logical source {}", sourceName);
    }
    const auto physicalSources = context.sourceCatalog()->getPhysicalSources(*logicalSource);
    if (!physicalSources || physicalSources->size() != 1)
    {
        throw TestException("Expected one physical source for {}", sourceName);
    }
    const auto file = physicalSources->begin()->tryGetFromConfig<std::string>("FILE_PATH");
    if (!file)
    {
        throw TestException("Physical source {} has no file_path", sourceName);
    }
    return *file;
}

}

TEST(SystestPreparationTest, FixtureContextsAreSharedWithinAFileAndIsolatedBetweenFiles)
{
    TemporaryWorkspace workspace{"systest-fixture-contexts"};
    auto first = parseTestFile(
        workspace.root,
        "a/shared.test",
        R"(GLOBALCONFIGURATION worker.mode: [B, A]
CREATE LOGICAL SOURCE input_a(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input_a TYPE File;
ATTACH INLINE
1
2

CREATE SINK output_a(value UINT64 NOT NULL) TYPE File;
SELECT value FROM input_a INTO output_a;
----
1
2
)");
    auto second = parseTestFile(
        workspace.root,
        "b/isolated.test",
        R"(CREATE LOGICAL SOURCE input_b(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input_b TYPE File;
ATTACH INLINE
9

CREATE SINK output_b(value UINT64 NOT NULL) TYPE File;
SELECT value FROM input_b INTO output_b;
----
9
)");

    auto resolved = resolveSystestFiles({std::move(second), std::move(first)}, oneWorkerCluster());
    ASSERT_TRUE(resolved) << resolved.error().what();
    ASSERT_EQ(resolved->environments.size(), 3);

    auto prepared = prepareSystestRun(std::move(*resolved), workspace.working, workspace.testData, QueryOptimizerConfiguration{});
    ASSERT_TRUE(prepared) << prepared.error().what();

    std::vector<EnvironmentId> sharedEnvironmentIds;
    std::vector<EnvironmentId> isolatedEnvironmentIds;
    for (const auto& environment : prepared->resolved.environments)
    {
        if (environment.relativeTestFile == "a/shared.test")
        {
            sharedEnvironmentIds.push_back(environment.id);
        }
        else if (environment.relativeTestFile == "b/isolated.test")
        {
            isolatedEnvironmentIds.push_back(environment.id);
        }
    }
    ASSERT_EQ(sharedEnvironmentIds.size(), 2);
    ASSERT_EQ(isolatedEnvironmentIds.size(), 1);

    const auto sharedFirst = prepared->environments->at(sharedEnvironmentIds[0]).context;
    const auto sharedSecond = prepared->environments->at(sharedEnvironmentIds[1]).context;
    const auto isolated = prepared->environments->at(isolatedEnvironmentIds[0]).context;
    ASSERT_NE(sharedFirst, nullptr);
    ASSERT_NE(isolated, nullptr);
    EXPECT_EQ(sharedFirst.get(), sharedSecond.get());
    EXPECT_NE(sharedFirst.get(), isolated.get());

    EXPECT_TRUE(sharedFirst->sourceCatalog()->containsLogicalSource(Identifier::parse("input_a")));
    EXPECT_FALSE(sharedFirst->sourceCatalog()->containsLogicalSource(Identifier::parse("input_b")));
    EXPECT_TRUE(sharedFirst->sinkCatalog()->containsSinkDescriptor(Identifier::parse("output_a")));
    EXPECT_FALSE(sharedFirst->sinkCatalog()->containsSinkDescriptor(Identifier::parse("output_b")));
    EXPECT_TRUE(isolated->sourceCatalog()->containsLogicalSource(Identifier::parse("input_b")));
    EXPECT_FALSE(isolated->sourceCatalog()->containsLogicalSource(Identifier::parse("input_a")));
    EXPECT_TRUE(isolated->sinkCatalog()->containsSinkDescriptor(Identifier::parse("output_b")));
    EXPECT_FALSE(isolated->sinkCatalog()->containsSinkDescriptor(Identifier::parse("output_a")));

    ASSERT_EQ(sharedFirst->generatedAttachments().size(), 1);
    ASSERT_EQ(isolated->generatedAttachments().size(), 1);
    EXPECT_NE(sharedFirst->generatedAttachments().front(), isolated->generatedAttachments().front());
    EXPECT_EQ(readFile(sharedFirst->generatedAttachments().front()), "1\n2\n");
    EXPECT_EQ(readFile(isolated->generatedAttachments().front()), "9\n");
    EXPECT_EQ(sharedFirst->sourceThreadCount(), 0);
    EXPECT_EQ(isolated->sourceThreadCount(), 0);
}

TEST(SystestPreparationTest, FixturesBecomeVisibleInSourceOrder)
{
    TemporaryWorkspace workspace{"systest-fixture-source-order"};
    auto parsed = parseTestFile(
        workspace.root,
        "suite/ordered-fixtures.test",
        R"(CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
1

SELECT value FROM input INTO later;
----
ERROR UnknownSinkName

CREATE SINK later(value UINT64 NOT NULL) TYPE File;

SELECT value FROM input INTO later;
----
1
)");

    auto resolved = resolveSystestFiles({std::move(parsed)}, oneWorkerCluster());
    ASSERT_TRUE(resolved) << resolved.error().what();
    auto prepared = prepareSystestRun(std::move(*resolved), workspace.working, workspace.testData, QueryOptimizerConfiguration{});
    ASSERT_TRUE(prepared) << prepared.error().what();

    const auto& first = std::get<PlanningFailure>(prepared->executions->at(id("suite/ordered-fixtures.test", 1, 0)).prepared);
    EXPECT_EQ(first.code, ErrorCode::UnknownSinkName);
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<const PreparedAction>>(
        prepared->executions->at(id("suite/ordered-fixtures.test", 2, 0)).prepared));
    const auto& context
        = prepared->environments->at(prepared->resolved.testCase(id("suite/ordered-fixtures.test", 2, 0)).environment).context;
    EXPECT_TRUE(context->sinkCatalog()->containsSinkDescriptor(Identifier::parse("later")));
}

TEST(SystestPreparationTest, NamedSinkPreservesHostSinkConfigurationAndFormatConfiguration)
{
    TemporaryWorkspace workspace{"systest-named-sink-configuration"};
    auto parsed = parseTestFile(
        workspace.root,
        "suite/named-sink.test",
        R"(GLOBALCONFIGURATION worker.mode: [A, B]
CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
1

CREATE SINK output(value UINT64 NOT NULL) TYPE File SET (
    'configured.csv' AS "SINK".FILE_PATH,
    'CSV' AS "SINK".OUTPUT_FORMAT,
    TRUE AS "SINK".APPEND,
    'sink-host:8080' AS "SINK".HOST,
    '|' AS "OUTPUT_FORMATTER".FIELD_DELIMITER
);
SELECT value FROM input INTO output;
----
1
)");

    const auto sourceHost = Host{"localhost:8080"};
    const auto sinkHost = Host{"sink-host:8080"};
    const auto cluster = SystestClusterConfiguration{
        .workers
        = {WorkerConfig{
               .host = sourceHost,
               .dataAddress = "localhost:9090",
               .maxOperators = CapacityKind::Limited{1000},
               .downstream = {sinkHost},
               .config = {}},
           WorkerConfig{
               .host = sinkHost,
               .dataAddress = "sink-host:9090",
               .maxOperators = CapacityKind::Limited{1000},
               .downstream = {},
               .config = {}}},
        .allowSourcePlacement = {sourceHost},
        .allowSinkPlacement = {sourceHost}};
    auto resolved = resolveSystestFiles({std::move(parsed)}, cluster);
    ASSERT_TRUE(resolved) << resolved.error().what();
    auto prepared = prepareSystestRun(std::move(*resolved), workspace.working, workspace.testData, QueryOptimizerConfiguration{});
    ASSERT_TRUE(prepared) << prepared.error().what();

    const auto& action = *preparedAction(*prepared->executions, id("suite/named-sink.test", 1, 0));
    const auto* query = std::get_if<PreparedQuery>(&action);
    ASSERT_NE(query, nullptr);
    const auto sinkOperator = query->statement.plan.getGlobalPlan().getRootOperators().front().tryGetAs<SinkLogicalOperator>();
    ASSERT_TRUE(sinkOperator);
    const auto descriptor = sinkOperator.value()->getSinkDescriptor();
    ASSERT_TRUE(descriptor);
    EXPECT_EQ(descriptor->getHost(), sinkHost);
    EXPECT_EQ(
        descriptor->getFromConfig(ConfigParametersFile::FILE_PATH),
        (workspace.working / "results/suite/named-sink%2Etest/query-1-variant-0-primary.csv").string());
    EXPECT_TRUE(descriptor->getFromConfig(ConfigParametersFile::APPEND));
    EXPECT_EQ(descriptor->getFromConfig(SinkDescriptor::OUTPUT_FORMAT), "CSV");
    const auto formatConfig = descriptor->getOutputFormatterConfig();
    ASSERT_TRUE(formatConfig.contains(Identifier::parse("field_delimiter")));
    EXPECT_EQ(formatConfig.at(Identifier::parse("field_delimiter")), "|");

    const auto& variantAction = *preparedAction(*prepared->executions, id("suite/named-sink.test", 1, 1));
    const auto* variantQuery = std::get_if<PreparedQuery>(&variantAction);
    ASSERT_NE(variantQuery, nullptr);
    const auto variantSinkOperator
        = variantQuery->statement.plan.getGlobalPlan().getRootOperators().front().tryGetAs<SinkLogicalOperator>();
    ASSERT_TRUE(variantSinkOperator);
    const auto variantDescriptor = variantSinkOperator.value()->getSinkDescriptor();
    ASSERT_TRUE(variantDescriptor);
    EXPECT_NE(descriptor->getSinkName(), variantDescriptor->getSinkName());
    EXPECT_NE(descriptor->getFromConfig(ConfigParametersFile::FILE_PATH), variantDescriptor->getFromConfig(ConfigParametersFile::FILE_PATH));
}

TEST(SystestPreparationTest, PreparesEveryActionWithDistinctVariantIdentity)
{
    TemporaryWorkspace workspace{"systest-action-preparation"};
    auto parsed = parseTestFile(
        workspace.root,
        "suite/actions.test",
        R"(GLOBALCONFIGURATION worker.mode: [B, A]
CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
1
2

SELECT value FROM input INTO File();
----
1
2

SELECT value FROM input INTO Checksum();
----
2 3

SELECT value FROM input INTO Void();
----

SELECT value FROM input INTO File();
====
SELECT value + UINT64(0) AS value FROM input INTO File();

EXPLAIN (LOGICAL) FORMAT TEXT SELECT value FROM input INTO File();
----
<REGEX>SOURCE\(INPUT\)</REGEX>
==END==

SELECT missing FROM input INTO File();
----
ERROR 2003

SELECT value FROM input INTO Void();
----
)");

    auto resolved = resolveSystestFiles({std::move(parsed)}, oneWorkerCluster());
    ASSERT_TRUE(resolved) << resolved.error().what();
    auto prepared = prepareSystestRun(std::move(*resolved), workspace.working, workspace.testData, QueryOptimizerConfiguration{});
    ASSERT_TRUE(prepared) << prepared.error().what();

    EXPECT_EQ(prepared->executions->ids(), prepared->resolved.ids());
    for (const auto& testCase : prepared->resolved.cases)
    {
        EXPECT_EQ(prepared->executions->at(testCase->id).environment, testCase->environment);
    }

    for (const uint64_t queryNumber : {1, 2, 3, 4, 7})
    {
        const auto& first = preparedAction(*prepared->executions, id("suite/actions.test", queryNumber, 0));
        const auto& second = preparedAction(*prepared->executions, id("suite/actions.test", queryNumber, 1));
        EXPECT_NE(first.get(), second.get());
    }

    const auto normalId = id("suite/actions.test", 1, 0);
    const auto& normalAction = *preparedAction(*prepared->executions, normalId);
    const auto* normal = std::get_if<PreparedQuery>(&normalAction);
    ASSERT_NE(normal, nullptr);
    EXPECT_EQ(normal->statement.output.kind, OutputTargetKind::Table);
    EXPECT_EQ(
        normal->statement.output.file,
        workspace.working / "results/suite/actions%2Etest/query-1-variant-0-primary.csv");
    const auto& normalVariant = *preparedAction(*prepared->executions, id("suite/actions.test", 1, 1));
    EXPECT_EQ(
        std::get<PreparedQuery>(normalVariant).statement.output.file,
        workspace.working / "results/suite/actions%2Etest/query-1-variant-1-primary.csv");
    EXPECT_NE(normal->statement.output.file, std::get<PreparedQuery>(normalVariant).statement.output.file);
    EXPECT_NE(normal->statement.plan.getQueryId(), std::get<PreparedQuery>(normalVariant).statement.plan.getQueryId());
    EXPECT_EQ(normal->statement.outputSchema.size(), 1);
    ASSERT_EQ(normal->statement.sourceMetrics.size(), 1);
    EXPECT_EQ(normal->statement.sourceMetrics.front().occurrences, 1);
    const auto& context = prepared->environments->at(prepared->resolved.testCase(normalId).environment).context;
    ASSERT_EQ(context->generatedAttachments().size(), 1);
    EXPECT_EQ(physicalSourceFile(*context, "input"), context->generatedAttachments().front());
    EXPECT_EQ(normal->statement.sourceMetrics.front().file, context->generatedAttachments().front());

    const auto& checksumAction = *preparedAction(*prepared->executions, id("suite/actions.test", 2, 0));
    const auto* checksum = std::get_if<PreparedQuery>(&checksumAction);
    ASSERT_NE(checksum, nullptr);
    EXPECT_EQ(checksum->statement.output.kind, OutputTargetKind::Table);
    EXPECT_EQ(
        checksum->statement.output.file,
        workspace.working / "results/suite/actions%2Etest/query-2-variant-0-primary.csv");
    EXPECT_EQ(checksum->statement.outputSchema, checksumSchema());

    const auto& voidAction = *preparedAction(*prepared->executions, id("suite/actions.test", 3, 0));
    const auto* voidQuery = std::get_if<PreparedQuery>(&voidAction);
    ASSERT_NE(voidQuery, nullptr);
    EXPECT_EQ(voidQuery->statement.output, (OutputTarget{.kind = OutputTargetKind::Discard, .file = {}}));

    const auto& differentialAction = *preparedAction(*prepared->executions, id("suite/actions.test", 4, 0));
    const auto* differential = std::get_if<PreparedDifferential>(&differentialAction);
    ASSERT_NE(differential, nullptr);
    EXPECT_EQ(differential->primary.output.kind, OutputTargetKind::Table);
    EXPECT_EQ(
        differential->primary.output.file,
        workspace.working / "results/suite/actions%2Etest/query-4-variant-0-primary.csv");
    EXPECT_EQ(differential->differential.output.kind, OutputTargetKind::Table);
    EXPECT_EQ(
        differential->differential.output.file,
        workspace.working / "results/suite/actions%2Etest/query-4-variant-0-differential.csv");
    EXPECT_NE(differential->primary.output.file, differential->differential.output.file);
    EXPECT_NE(differential->primary.plan.getQueryId(), differential->differential.plan.getQueryId());

    const auto& explainAction = *preparedAction(*prepared->executions, id("suite/actions.test", 5, 0));
    const auto* explain = std::get_if<PreparedExplain>(&explainAction);
    ASSERT_NE(explain, nullptr);
    EXPECT_TRUE(explain->output.contains("== Initial Logical Plan =="));
    EXPECT_TRUE(explain->output.contains("SOURCE(INPUT)"));

    const auto& firstFailure = std::get<PlanningFailure>(prepared->executions->at(id("suite/actions.test", 6, 0)).prepared);
    const auto& secondFailure = std::get<PlanningFailure>(prepared->executions->at(id("suite/actions.test", 6, 1)).prepared);
    EXPECT_EQ(firstFailure.code, ErrorCode::CannotInferSchema);
    EXPECT_FALSE(firstFailure.message.empty());
    EXPECT_EQ(secondFailure.code, firstFailure.code);
    EXPECT_FALSE(secondFailure.message.empty());

    const auto& actionAfterFailure = *preparedAction(*prepared->executions, id("suite/actions.test", 7, 0));
    const auto* queryAfterFailure = std::get_if<PreparedQuery>(&actionAfterFailure);
    ASSERT_NE(queryAfterFailure, nullptr);
    EXPECT_EQ(queryAfterFailure->statement.output.kind, OutputTargetKind::Discard);
}

TEST(SystestPreparationTest, AttachedFilesStayInTheTestDataDirectory)
{
    TemporaryWorkspace workspace{"systest-file-attachment"};
    std::filesystem::create_directories(workspace.testData / "inputs");
    const auto inputFile = workspace.testData / "inputs/source.csv";
    std::ofstream(inputFile) << "1\n2\n";
    auto parsed = parseTestFile(
        workspace.root,
        "suite/file.test",
        R"(CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH FILE inputs/source.csv

SELECT value FROM input INTO File();
----
1
2
)");

    auto resolved = resolveSystestFiles({std::move(parsed)}, oneWorkerCluster());
    ASSERT_TRUE(resolved) << resolved.error().what();
    auto prepared = prepareSystestRun(std::move(*resolved), workspace.working, workspace.testData, QueryOptimizerConfiguration{});
    ASSERT_TRUE(prepared) << prepared.error().what();

    const auto testCase = id("suite/file.test", 1, 0);
    const auto& context = prepared->environments->at(prepared->resolved.testCase(testCase).environment).context;
    EXPECT_TRUE(context->generatedAttachments().empty());
    EXPECT_EQ(context->sourceThreadCount(), 0);
    const auto& action = *preparedAction(*prepared->executions, testCase);
    EXPECT_TRUE(std::holds_alternative<PreparedQuery>(action));
    EXPECT_EQ(physicalSourceFile(*context, "input"), inputFile);
    EXPECT_EQ(readFile(inputFile), "1\n2\n");
}

TEST(SystestPreparationTest, SameStemFilesAndEscapingRelativePathsProduceDistinctContainedArtifacts)
{
    TemporaryWorkspace workspace{"systest-artifact-identity"};
    auto first = parseTestFile(
        workspace.root,
        "left/same.test",
        R"(CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
1

SELECT value FROM input INTO File();
----
1
)");
    auto second = parseTestFile(
        workspace.root,
        "right/same.test",
        R"(CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
2

SELECT value FROM input INTO File();
----
2
)");
    const auto escapingFile = workspace.root / "escaping.test";
    std::ofstream(escapingFile) << R"(CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
3

SELECT value FROM input INTO File();
----
3
)";
    SystestParser parser;
    ASSERT_TRUE(parser.loadFile(escapingFile, "../../outside/same.test"));
    auto escaping = parser.parse();

    auto resolved = resolveSystestFiles({std::move(second), std::move(escaping), std::move(first)}, oneWorkerCluster());
    ASSERT_TRUE(resolved) << resolved.error().what();
    auto prepared = prepareSystestRun(std::move(*resolved), workspace.working, workspace.testData, QueryOptimizerConfiguration{});
    ASSERT_TRUE(prepared) << prepared.error().what();

    const auto outputFor = [&](const TestCaseId& testCaseId)
    {
        const auto& action = *preparedAction(*prepared->executions, testCaseId);
        return std::get<PreparedQuery>(action).statement.output.file;
    };
    const auto left = outputFor(id("left/same.test", 1, 0));
    const auto right = outputFor(id("right/same.test", 1, 0));
    const auto escapingOutput = outputFor(id("../../outside/same.test", 1, 0));
    const auto queryIdFor = [&](const TestCaseId& testCaseId)
    {
        const auto& action = *preparedAction(*prepared->executions, testCaseId);
        return std::get<PreparedQuery>(action).statement.plan.getQueryId();
    };
    EXPECT_NE(queryIdFor(id("left/same.test", 1, 0)), queryIdFor(id("right/same.test", 1, 0)));
    EXPECT_NE(queryIdFor(id("left/same.test", 1, 0)), queryIdFor(id("../../outside/same.test", 1, 0)));
    EXPECT_NE(left, right);
    EXPECT_NE(left, escapingOutput);
    EXPECT_NE(right, escapingOutput);
    for (const auto& output : {left, right, escapingOutput})
    {
        const auto relative = output.lexically_normal().lexically_relative(workspace.working.lexically_normal());
        ASSERT_FALSE(relative.empty());
        EXPECT_NE(*relative.begin(), std::filesystem::path{".."});
        EXPECT_EQ(relative.begin()->string(), "results");
    }
}

TEST(SystestPreparationTest, ExplicitPhysicalSourceHostDoesNotRequireDefaultPlacement)
{
    TemporaryWorkspace workspace{"systest-explicit-source-host"};
    auto parsed = parseTestFile(
        workspace.root,
        "suite/explicit-source.test",
        R"(CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File SET (
    'source-host:8080' AS "SOURCE"."HOST"
);
ATTACH INLINE
1

SELECT value FROM input INTO File();
----
1
)");
    const auto sourceHost = Host{"source-host:8080"};
    const auto cluster = SystestClusterConfiguration{
        .workers = {WorkerConfig{
            .host = sourceHost,
            .dataAddress = "source-host:9090",
            .maxOperators = CapacityKind::Limited{1000},
            .downstream = {},
            .config = {}}},
        .allowSourcePlacement = {},
        .allowSinkPlacement = {sourceHost}};

    auto resolved = resolveSystestFiles({std::move(parsed)}, cluster);
    ASSERT_TRUE(resolved) << resolved.error().what();
    auto prepared = prepareSystestRun(std::move(*resolved), workspace.working, workspace.testData, QueryOptimizerConfiguration{});

    ASSERT_TRUE(prepared) << prepared.error().what();
    const auto testCaseId = id("suite/explicit-source.test", 1, 0);
    const auto& context = prepared->environments->at(prepared->resolved.testCase(testCaseId).environment).context;
    const auto logicalSource = context->sourceCatalog()->getLogicalSource(Identifier::parse("input"));
    ASSERT_TRUE(logicalSource);
    const auto physicalSources = context->sourceCatalog()->getPhysicalSources(*logicalSource);
    ASSERT_TRUE(physicalSources);
    ASSERT_EQ(physicalSources->size(), 1);
    EXPECT_EQ(physicalSources->begin()->getHost(), sourceHost);
}

TEST(SystestPreparationTest, PhysicalSourceWithoutHostRequiresDefaultPlacement)
{
    TemporaryWorkspace workspace{"systest-missing-source-host"};
    auto parsed = parseTestFile(
        workspace.root,
        "suite/missing-source-host.test",
        R"(CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
1
)");
    const auto workerHost = Host{"worker:8080"};
    const auto cluster = SystestClusterConfiguration{
        .workers = {WorkerConfig{
            .host = workerHost,
            .dataAddress = "worker:9090",
            .maxOperators = CapacityKind::Limited{1000},
            .downstream = {},
            .config = {}}},
        .allowSourcePlacement = {},
        .allowSinkPlacement = {workerHost}};

    auto resolved = resolveSystestFiles({std::move(parsed)}, cluster);
    ASSERT_TRUE(resolved) << resolved.error().what();
    auto prepared = prepareSystestRun(std::move(*resolved), workspace.working, workspace.testData, QueryOptimizerConfiguration{});

    ASSERT_FALSE(prepared);
    EXPECT_TRUE(std::string{prepared.error().what()}.contains("allow_source_placement"));
}

TEST(SystestPreparationTest, FixtureOnlyFilesArePreparedAndFailTheRun)
{
    TemporaryWorkspace workspace{"systest-fixture-only-file"};
    auto fixtureOnly = parseTestFile(
        workspace.root,
        "suite/fixture-only.test",
        R"(CREATE PHYSICAL SOURCE FOR missing TYPE File;
)");
    auto runnable = parseTestFile(
        workspace.root,
        "suite/runnable.test",
        R"(CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
1

SELECT value FROM input INTO File();
----
1
)");

    auto resolved = resolveSystestFiles({std::move(fixtureOnly), std::move(runnable)}, oneWorkerCluster());
    ASSERT_TRUE(resolved) << resolved.error().what();
    ASSERT_EQ(resolved->environments.size(), 2);
    auto prepared = prepareSystestRun(std::move(*resolved), workspace.working, workspace.testData, QueryOptimizerConfiguration{});

    ASSERT_FALSE(prepared);
    EXPECT_EQ(prepared.error().code(), ErrorCode::UnknownSourceName);
}

TEST(SystestPreparationTest, FixtureFailureAbortsTheRunBeforeCasePreparation)
{
    TemporaryWorkspace workspace{"systest-fatal-fixture"};
    auto parsed = parseTestFile(
        workspace.root,
        "suite/fatal.test",
        R"(CREATE PHYSICAL SOURCE FOR missing TYPE File;

SELECT 1 INTO File();
----
1
)");

    auto resolved = resolveSystestFiles({std::move(parsed)}, oneWorkerCluster());
    ASSERT_TRUE(resolved) << resolved.error().what();
    auto prepared = prepareSystestRun(std::move(*resolved), workspace.working, workspace.testData, QueryOptimizerConfiguration{});

    ASSERT_FALSE(prepared);
    EXPECT_EQ(prepared.error().code(), ErrorCode::UnknownSourceName);
    EXPECT_TRUE(std::string{prepared.error().what()}.contains("MISSING"));
}

}
