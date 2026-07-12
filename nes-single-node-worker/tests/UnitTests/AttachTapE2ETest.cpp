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
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <Configuration/WorkerConfiguration.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/StatisticListener.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <CompiledQueryPlan.hpp>
#include <QueryCompiler.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <QueryStatus.hpp>

namespace NES
{
namespace
{

/// 16 bytes per input row ("%06d,%08d\n") so that ROWS_PER_BUFFER rows fill exactly one 4096-byte raw
/// source buffer: FileSource reads full buffers from the FIFO, which makes phase boundaries deterministic.
constexpr size_t ROW_WIDTH = 16;
constexpr size_t ROWS_PER_BUFFER = DEFAULT_OPERATOR_BUFFER_SIZE / ROW_WIDTH;
/// Generous margin for the asynchronous attach/detach operations to take effect (each is a few engine tasks).
constexpr auto ASYNC_MARGIN = std::chrono::seconds(1);

QueryId randomQueryId()
{
    return QueryId::createLocal(LocalQueryId(generateUUID()));
}

struct NoopStatisticListener final : StatisticListener
{
    void onEvent(Event) override { }

    void onEvent(SystemEvent) override { }
};

class AttachTapE2ETest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("AttachTapE2ETest.log", LogLevel::LOG_DEBUG); }

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

    /// Main query: FIFO file source (CSV) -> shared selection -> TWO file sinks (static fan-out).
    /// The two sinks matter for tappability: at the fan-out point the shared selection pipeline is closed
    /// with a NATIVE emit and per-branch formatting pipelines. With a single CSV sink, the pipelining phase
    /// would fuse the CSV formatting emit INTO the selection pipeline, and a tap attached there would read
    /// formatted bytes instead of native tuples.
    LogicalPlan
    makeMainPlan(const std::filesystem::path& fifoPath, const std::filesystem::path& sinkAPath, const std::filesystem::path& sinkBPath)
    {
        auto sourceDescriptor = sourceCatalog.getInlineSource(
            "File", createSchema(), Host("localhost"), {{"type", "CSV"}}, {{"file_path", fifoPath.string()}});
        EXPECT_TRUE(sourceDescriptor.has_value());
        const auto source = LogicalOperator{SourceDescriptorLogicalOperator{std::move(sourceDescriptor.value())}}
                                /// NOLINT(bugprone-unchecked-optional-access)
                                .withTraitSet(TraitSet{
                                    OutputOriginIdsTrait{std::vector{OriginId(1)}}, MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT}});

        auto selection = LogicalOperator{SelectionLogicalOperator{GreaterEqualsLogicalFunction(
                                             FieldAccessLogicalFunction("test.id"),
                                             ConstantValueLogicalFunction(DataTypeProvider::provideDataType(DataType::Type::UINT64), "0"))}}
                             .withChildren({source});
        selection = selection.withInferredSchema({source.getOutputSchema()});
        selection = selection.withTraitSet(TraitSet{MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT}});

        auto makeSink = [&](const std::filesystem::path& path)
        {
            auto sinkDescriptor = sinkCatalog.getInlineSink(
                createSchema(), "File", Host("localhost"), {{"output_format", "CSV"}, {"file_path", path.string()}}, {});
            EXPECT_TRUE(sinkDescriptor.has_value());
            return LogicalOperator{SinkLogicalOperator{sinkDescriptor.value()}} /// NOLINT(bugprone-unchecked-optional-access)
                .withChildren({selection})
                .withTraitSet(TraitSet{MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT}});
        };
        return LogicalPlan(randomQueryId(), {makeSink(sinkAPath), makeSink(sinkBPath)});
    }

    /// Tap branch: placeholder NATIVE file source (describes the tapped data, never started) -> file sink.
    LogicalPlan makeTapPlan(const std::filesystem::path& sinkPath)
    {
        auto sourceDescriptor
            = sourceCatalog.getInlineSource("File", createSchema(), Host("localhost"), {{"type", "Native"}}, {{"file_path", "/dev/null"}});
        EXPECT_TRUE(sourceDescriptor.has_value());
        const auto placeholder
            = LogicalOperator{SourceDescriptorLogicalOperator{std::move(sourceDescriptor.value())}}
                  /// NOLINT(bugprone-unchecked-optional-access)
                  .withTraitSet(
                      TraitSet{OutputOriginIdsTrait{std::vector{OriginId(1)}}, MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT}});

        auto sinkDescriptor = sinkCatalog.getInlineSink(
            createSchema(), "File", Host("localhost"), {{"output_format", "CSV"}, {"file_path", sinkPath.string()}}, {});
        EXPECT_TRUE(sinkDescriptor.has_value());
        const auto sink = LogicalOperator{SinkLogicalOperator{sinkDescriptor.value()}} /// NOLINT(bugprone-unchecked-optional-access)
                              .withChildren({placeholder})
                              .withTraitSet(TraitSet{MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT}});
        return LogicalPlan(randomQueryId(), {sink});
    }

    static std::unique_ptr<CompiledQueryPlan> compile(LogicalPlan plan)
    {
        auto compiler = QueryCompilation::QueryCompiler{QueryExecutionConfiguration{}};
        return compiler.compileQuery(std::make_unique<QueryCompilation::QueryCompilationRequest>(
            QueryCompilation::QueryCompilationRequest{.queryPlan = std::move(plan)}));
    }

    /// Writes `count` fixed-width rows [startId, startId+count) into the FIFO (multiples of full buffers).
    static void writeRows(const int fd, const uint64_t startId, const size_t count)
    {
        std::string payload;
        payload.reserve(count * ROW_WIDTH);
        for (uint64_t id = startId; id < startId + count; ++id)
        {
            payload += fmt::format("{:06},{:08}\n", id, id * 2);
        }
        ASSERT_EQ(payload.size(), count * ROW_WIDTH);
        ASSERT_EQ(::write(fd, payload.data(), payload.size()), static_cast<ssize_t>(payload.size()));
    }

    static std::set<uint64_t> readSinkIds(const std::filesystem::path& path)
    {
        std::set<uint64_t> ids;
        std::ifstream file(path);
        std::string header;
        std::getline(file, header); /// skip the schema header line
        for (std::string line; std::getline(file, line);)
        {
            if (not line.empty())
            {
                ids.insert(std::stoull(line.substr(0, line.find(','))));
            }
        }
        return ids;
    }

    testing::AssertionResult waitForStatus(const NodeEngine& engine, const QueryId queryId, const QueryStatus expected) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (const auto status = engine.getQueryLog()->getQueryStatus(queryId); status && status->state == expected)
            {
                return testing::AssertionSuccess();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        return testing::AssertionFailure() << "query did not reach status " << magic_enum::enum_name(expected);
    }

    std::filesystem::path testDir;
    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
};

/// End-to-end runtime attachment: a running query (FIFO source -> selection -> sinkA) gets a tap branch
/// (-> sinkB) attached to its selection pipeline mid-stream and detached again later.
/// Phases are separated by full source buffers and generous time margins:
///   phase1 -> attach -> phase2 -> detach -> phase3 -> EOS.
/// Expected: sinkA sees ALL phases; the tap sees EXACTLY phase2 (stream suffix while attached).
TEST_F(AttachTapE2ETest, TapAttachedToRunningQueryReceivesStreamSuffix)
{
    const auto fifoPath = testDir / "input.fifo";
    const auto sinkAPath = testDir / "sinkA.csv";
    const auto sinkBPath = testDir / "sinkB.csv";
    const auto tapSinkPath = testDir / "tap.csv";
    ASSERT_EQ(::mkfifo(fifoPath.c_str(), 0600), 0);
    /// O_RDWR never blocks on a FIFO and keeps a writer alive until we close it (which signals EOS).
    const int fifoFd = ::open(fifoPath.c_str(), O_RDWR);
    ASSERT_GE(fifoFd, 0);

    auto mainPlan = makeMainPlan(fifoPath, sinkAPath, sinkBPath);
    const auto mainQueryId = mainPlan.getQueryId();
    auto compiledMain = compile(std::move(mainPlan));
    ASSERT_TRUE(compiledMain);
    ASSERT_EQ(compiledMain->sources.size(), 1U);
    ASSERT_EQ(compiledMain->sources.front().successors.size(), 1U);
    const auto targetPipeline = compiledMain->sources.front().successors.front().lock();
    ASSERT_TRUE(targetPipeline);
    const auto targetPipelineId = targetPipeline->id;

    auto compiledTap = compile(makeTapPlan(tapSinkPath));
    ASSERT_TRUE(compiledTap);

    const auto engine = NodeEngineBuilder(WorkerConfiguration{}, std::make_shared<NoopStatisticListener>()).build(Host("localhost"));
    engine->registerCompiledQueryPlan(mainQueryId, std::move(compiledMain));
    engine->startQuery(mainQueryId);
    ASSERT_TRUE(waitForStatus(*engine, mainQueryId, QueryStatus::Running));

    /// Phase 1: only sinkA is connected.
    writeRows(fifoFd, 0, ROWS_PER_BUFFER);
    std::this_thread::sleep_for(ASYNC_MARGIN);

    /// Attach the tap to the running selection pipeline.
    const auto attachedPipelineId = engine->attachToQuery(mainQueryId, targetPipelineId, std::move(compiledTap));
    std::this_thread::sleep_for(ASYNC_MARGIN);

    /// Phase 2: both sinks are connected.
    writeRows(fifoFd, ROWS_PER_BUFFER, ROWS_PER_BUFFER);
    std::this_thread::sleep_for(ASYNC_MARGIN);

    /// Detach the tap again; the branch terminates gracefully while the query keeps running.
    engine->detachFromQuery(mainQueryId, targetPipelineId, attachedPipelineId);
    std::this_thread::sleep_for(ASYNC_MARGIN);

    /// Phase 3: only sinkA is connected again.
    writeRows(fifoFd, 2 * ROWS_PER_BUFFER, ROWS_PER_BUFFER);
    std::this_thread::sleep_for(ASYNC_MARGIN);

    /// Closing the write end signals EOS; the query terminates gracefully.
    ASSERT_EQ(::close(fifoFd), 0);
    ASSERT_TRUE(waitForStatus(*engine, mainQueryId, QueryStatus::Stopped));

    const auto sinkAIds = readSinkIds(sinkAPath);
    const auto tapIds = readSinkIds(tapSinkPath);

    /// The main sink received the complete stream.
    EXPECT_EQ(sinkAIds.size(), 3 * ROWS_PER_BUFFER);
    EXPECT_EQ(*sinkAIds.begin(), 0U);
    EXPECT_EQ(*sinkAIds.rbegin(), (3 * ROWS_PER_BUFFER) - 1);

    /// The tap received exactly the stream suffix that flowed while it was attached (phase 2).
    std::set<uint64_t> expectedTapIds;
    for (uint64_t id = ROWS_PER_BUFFER; id < 2 * ROWS_PER_BUFFER; ++id)
    {
        expectedTapIds.insert(id);
    }
    EXPECT_EQ(tapIds, expectedTapIds);
}

}
}
