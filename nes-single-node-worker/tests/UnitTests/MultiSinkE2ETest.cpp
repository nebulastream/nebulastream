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

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{
namespace
{

constexpr uint64_t NUM_INPUT_TUPLES = 100;

QueryId randomQueryId()
{
    return QueryId::createLocal(LocalQueryId(generateUUID()));
}

/// End-to-end fan-out test: one file source, a shared selection, and TWO file sinks in a single query.
/// The plan is built as a DAG directly (bypassing the optimizer, whose rebuilds would regenerate operator
/// ids and thereby split the shared subplan) and submitted via SingleNodeWorker::registerQuery.
class MultiSinkE2ETest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("MultiSinkE2ETest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        testDir = std::filesystem::path(::testing::TempDir()) / ::testing::UnitTest::GetInstance()->current_test_info()->name();
        std::filesystem::remove_all(testDir);
        std::filesystem::create_directories(testDir);
    }

    static Schema createSchema()
    {
        Schema schema;
        schema.addField("test.id", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        schema.addField("test.value", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        return schema;
    }

    std::filesystem::path writeInputFile() const
    {
        const auto path = testDir / "input.csv";
        std::ofstream input(path);
        for (uint64_t i = 0; i < NUM_INPUT_TUPLES; ++i)
        {
            input << i << "," << (i * 2) << "\n";
        }
        input.close();
        return path;
    }

    LogicalOperator makeBoundSource(const std::filesystem::path& inputPath)
    {
        auto descriptor = sourceCatalog.getInlineSource(
            "File", createSchema(), Host("localhost"), {{"type", "CSV"}}, {{"file_path", inputPath.string()}});
        EXPECT_TRUE(descriptor.has_value());
        return LogicalOperator{SourceDescriptorLogicalOperator{std::move(descriptor.value())}}
            /// NOLINT(bugprone-unchecked-optional-access)
            .withTraitSet(TraitSet{OutputOriginIdsTrait{std::vector{OriginId(1)}}, MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT}});
    }

    LogicalOperator makeBoundFileSink(const LogicalOperator& child, const std::filesystem::path& outputPath)
    {
        auto descriptor = sinkCatalog.getInlineSink(
            createSchema(), "File", Host("localhost"), {{"output_format", "CSV"}, {"file_path", outputPath.string()}}, {});
        EXPECT_TRUE(descriptor.has_value());
        return LogicalOperator{SinkLogicalOperator{descriptor.value()}} /// NOLINT(bugprone-unchecked-optional-access)
            .withChildren({child})
            .withTraitSet(TraitSet{MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT}});
    }

    /// Reads the data lines of a CSV sink output (skipping the leading schema header line), sorted.
    static std::vector<std::string> readSortedDataLines(const std::filesystem::path& path)
    {
        std::vector<std::string> lines;
        std::ifstream file(path);
        std::string header;
        std::getline(file, header);
        EXPECT_TRUE(header.contains("test.id")) << "expected a schema header line, got: " << header;
        for (std::string line; std::getline(file, line);)
        {
            if (not line.empty())
            {
                lines.push_back(line);
            }
        }
        std::ranges::sort(lines);
        return lines;
    }

    /// Runs the plan to completion (file sources terminate on EOF) and waits for the Stopped state.
    static void runQueryToCompletion(SingleNodeWorker& worker, LogicalPlan plan)
    {
        const auto queryId = worker.registerQuery(std::move(plan));
        ASSERT_TRUE(queryId.has_value()) << "registerQuery failed: " << (queryId ? "" : queryId.error().what());
        const auto started = worker.startQuery(*queryId);
        ASSERT_TRUE(started.has_value());

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            const auto status = worker.getQueryStatus(*queryId);
            ASSERT_TRUE(status.has_value());
            ASSERT_NE(status->state, QueryStatus::Failed) << "query failed during execution";
            if (status->state == QueryStatus::Stopped)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        FAIL() << "query did not reach Stopped state within the deadline";
    }

    std::filesystem::path testDir;
    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
};

/// source -> shared selection -> {sinkA, sinkB}: pipeline-level fan-out. Both sinks must receive the
/// identical, complete stream.
TEST_F(MultiSinkE2ETest, SharedSelectionFansOutToTwoFileSinks)
{
    const auto inputPath = writeInputFile();
    const auto outputPathA = testDir / "sinkA.csv";
    const auto outputPathB = testDir / "sinkB.csv";

    const auto source = makeBoundSource(inputPath);
    auto selection = LogicalOperator{SelectionLogicalOperator{GreaterEqualsLogicalFunction(
                                         FieldAccessLogicalFunction("test.id"),
                                         ConstantValueLogicalFunction(DataTypeProvider::provideDataType(DataType::Type::UINT64), "0"))}}
                         .withChildren({source});
    selection = selection.withInferredSchema({source.getOutputSchema()});
    selection = selection.withTraitSet(TraitSet{MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT}});

    const auto sinkA = makeBoundFileSink(selection, outputPathA);
    const auto sinkB = makeBoundFileSink(selection, outputPathB);

    SingleNodeWorker worker{SingleNodeWorkerConfiguration{}};
    runQueryToCompletion(worker, LogicalPlan(randomQueryId(), {sinkA, sinkB}));

    const auto linesA = readSortedDataLines(outputPathA);
    const auto linesB = readSortedDataLines(outputPathB);
    EXPECT_EQ(linesA.size(), NUM_INPUT_TUPLES);
    EXPECT_EQ(linesA, linesB);
}

/// source -> {sinkA, sinkB}: source-level fan-out (control case without a shared operator pipeline).
TEST_F(MultiSinkE2ETest, SourceFansOutToTwoFileSinks)
{
    const auto inputPath = writeInputFile();
    const auto outputPathA = testDir / "sinkA.csv";
    const auto outputPathB = testDir / "sinkB.csv";

    const auto source = makeBoundSource(inputPath);
    const auto sinkA = makeBoundFileSink(source, outputPathA);
    const auto sinkB = makeBoundFileSink(source, outputPathB);

    SingleNodeWorker worker{SingleNodeWorkerConfiguration{}};
    runQueryToCompletion(worker, LogicalPlan(randomQueryId(), {sinkA, sinkB}));

    const auto linesA = readSortedDataLines(outputPathA);
    const auto linesB = readSortedDataLines(outputPathB);
    EXPECT_EQ(linesA.size(), NUM_INPUT_TUPLES);
    EXPECT_EQ(linesA, linesB);
}

}
}
