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

#include <SemanticMapPhysicalOperator.hpp>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <Pipeline.hpp>
#include <PipelineExecutionContext.hpp>
#include <ScanPhysicalOperator.hpp>
#include <SemanticBackend.hpp>
#include <SemanticBackendFactory.hpp>
#include <SemanticModelCatalog.hpp>
#include <TestTupleBuffer.hpp>
#include <options.hpp>

namespace NES
{

/// NOLINTBEGIN(readability-magic-numbers, bugprone-unchecked-optional-access)

namespace
{

constexpr size_t BufferSize = 8192;
constexpr uint32_t NumberOfPooledBuffers = 200;
constexpr BufferAlignment Alignment{64};
constexpr double UnpooledMemoryFraction = 0.9;
constexpr size_t TotalMemoryInBytes = 10 * static_cast<size_t>(NumberOfPooledBuffers) * BufferSize;

using TestSchema = Schema<UnqualifiedUnboundField, Ordered>;
using Row = std::unordered_map<std::string, std::string>;

std::shared_ptr<BufferManager> createBufferManager(const size_t totalMemory = TotalMemoryInBytes)
{
    return BufferManager::create(totalMemory, UnpooledMemoryFraction, Alignment, BufferSize, std::make_shared<NesDefaultMemoryAllocator>());
}

UnqualifiedUnboundField varsized(const std::string_view name)
{
    return UnqualifiedUnboundField{
        Identifier::parse(std::string{name}), DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}};
}

std::vector<QualifiedIdentifier> toIds(const std::vector<std::string>& names)
{
    return names | std::views::transform([](const std::string& name) { return QualifiedIdentifier::tryParse(name).value(); })
        | std::ranges::to<std::vector>();
}

/// A one-step model; `backend`/`endpoint` pick the mock behaviour when the real factory is used.
SemanticModelConfig modelConfig(std::string endpoint, std::vector<std::string> outputValues = {}, std::string defaultValue = "")
{
    SemanticModelConfig config;
    config.backend = "mock";
    config.endpoint = std::move(endpoint);
    config.modelName = "mock-model";
    config.steps = {SemanticStep{
        .kind = SemanticStep::Kind::MAP,
        .prompt = "Classify the sentiment",
        .outputColumn = "SENTIMENT",
        .outputValues = std::move(outputValues),
        .defaultValue = std::move(defaultValue)}};
    return config;
}

SemanticBackendProvider realFactory(const SemanticModelConfig& config)
{
    return [config] { return SemanticBackendFactory::create(config, std::nullopt); };
}

/// Answers every request with a malformed chat completion (a 2xx that is no envelope).
struct MalformedBackend final : SemanticBackend
{
    std::expected<std::string, BackendError> complete(const CompletionRequest&) override
    {
        return std::unexpected{BackendError{.kind = BackendError::Kind::MALFORMED_RESPONSE, .message = "no envelope"}};
    }
};

/// Every instance gets a unique id and answers with it, proving each worker thread uses its own backend.
struct CountingBackend final : SemanticBackend
{
    static inline std::atomic<int> nextId{0};
    int id = nextId.fetch_add(1);

    std::expected<std::string, BackendError> complete(const CompletionRequest&) override
    {
        return fmt::format(R"({{"row1": {{"sentiment": {{"answer": "{}"}}}}}})", id);
    }
};

/// Records the highest number of requests that were in flight at the same time.
struct SlowBackend final : SemanticBackend
{
    static inline std::atomic<int> inFlight{0};
    static inline std::atomic<int> maxInFlight{0};

    std::expected<std::string, BackendError> complete(const CompletionRequest&) override
    {
        const auto now = inFlight.fetch_add(1) + 1;
        auto seen = maxInFlight.load();
        while (now > seen && !maxInFlight.compare_exchange_weak(seen, now))
        {
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        inFlight.fetch_sub(1);
        return R"({"row1": {"sentiment": {"answer": "done"}}})";
    }
};

struct MockedPipelineContext final : PipelineExecutionContext
{
    bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
    {
        buffers.wlock()->emplace_back(buffer);
        return true;
    }

    TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return threadId; }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return numberOfWorkerThreads; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return *operatorHandlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& handlers) override
    {
        operatorHandlers = &handlers;
    }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { INVARIANT(false, "This function should not be called"); }

    ///NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) lifetime is ensured by the test
    folly::Synchronized<std::vector<TupleBuffer>>& buffers;
    std::shared_ptr<BufferManager> bufferManager;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* operatorHandlers = nullptr;
    WorkerThreadId threadId;
    uint64_t numberOfWorkerThreads;

    MockedPipelineContext(
        folly::Synchronized<std::vector<TupleBuffer>>& buffers,
        std::shared_ptr<BufferManager> bufferManager,
        const WorkerThreadId threadId = INITIAL<WorkerThreadId>,
        const uint64_t numberOfWorkerThreads = 1)
        : buffers(buffers), bufferManager(std::move(bufferManager)), threadId(threadId), numberOfWorkerThreads(numberOfWorkerThreads)
    {
    }
};

nautilus::engine::Options engineOptions(const bool compiled)
{
    nautilus::engine::Options options;
    options.setOption("engine.Compilation", compiled);
    options.setOption("engine.backend", std::string("mlir"));
    options.setOption("engine.compilationStrategy", std::string("legacy"));
    return options;
}

/// Scan -> SemanticMap -> Emit over `inputNames` (all VARSIZED), appending one SENTIMENT field.
class SemanticMapPipeline
{
public:
    SemanticMapPipeline(SemanticBackendProvider provider, const SemanticModelConfig& config, const std::vector<std::string>& inputNames)
        : inputSchema(inputNames | std::views::transform(varsized) | std::ranges::to<TestSchema>())
        , outputSchema(
              [&inputNames]
              {
                  auto fields = inputNames | std::views::transform(varsized) | std::ranges::to<std::vector>();
                  fields.push_back(varsized("SENTIMENT"));
                  return fields | std::ranges::to<TestSchema>();
              }())
        , inputNames(inputNames)
    {
        const auto toSchemaIds = [](const TestSchema& schema)
        {
            return schema
                | std::views::transform([](const UnqualifiedUnboundField& field)
                                        { return static_cast<QualifiedIdentifier>(field.getFullyQualifiedName()); })
                | std::ranges::to<std::vector>();
        };
        ScanPhysicalOperator scan(
            LowerSchemaProvider::lowerSchema(BufferSize, inputSchema, MemoryLayoutType::ROW_LAYOUT), toSchemaIds(inputSchema));
        SemanticMapPhysicalOperator semanticMap(std::move(provider), config, toIds(inputNames), toIds({"SENTIMENT"}));
        const EmitPhysicalOperator emit(
            OperatorHandlerId(1), LowerSchemaProvider::lowerSchema(BufferSize, outputSchema, MemoryLayoutType::ROW_LAYOUT));
        semanticMap.setChild(PhysicalOperator(emit));
        scan.setChild(PhysicalOperator(semanticMap));
        pipeline = std::make_shared<Pipeline>(PhysicalOperator(scan));
        handlers[OperatorHandlerId(1)] = std::make_shared<EmitOperatorHandler>();
    }

    [[nodiscard]] TupleBuffer
    inputBuffer(const std::vector<std::vector<std::string>>& rows, const SequenceNumber sequence = SequenceNumber(1)) const
    {
        const auto tupleBuffer = inputBufferManager->getBufferBlocking();
        auto buffer = tupleBuffer;
        buffer.setSequenceNumber(sequence);
        buffer.setChunkNumber(INITIAL_CHUNK_NUMBER);
        buffer.setLastChunk(true);
        buffer.setOriginId(INITIAL<OriginId>);
        Testing::TestTupleBuffer testBuffer(inputSchema);
        auto view = testBuffer.open(buffer, inputBufferManager.get());
        for (const auto& row : rows)
        {
            if (row.size() == 1)
            {
                view.append(row[0]);
            }
            else
            {
                view.append(row[0], row[1]);
            }
        }
        return buffer;
    }

    /// Runs `rows` through a fresh stage on one worker thread and returns the emitted records.
    [[nodiscard]] std::vector<Row> run(const std::vector<std::vector<std::string>>& rows, const bool compiled)
    {
        const auto input = inputBuffer(rows);
        CompiledExecutablePipelineStage stage(pipeline, handlers, engineOptions(compiled));
        folly::Synchronized<std::vector<TupleBuffer>> emitted;
        MockedPipelineContext pec{emitted, createBufferManager()};
        stage.start(pec);
        stage.execute(input, pec);
        stage.stop(pec);
        return records(*emitted.rlock());
    }

    [[nodiscard]] std::vector<Row> records(const std::vector<TupleBuffer>& buffers) const
    {
        std::vector<Row> result;
        for (auto buffer : buffers)
        {
            Testing::TestTupleBuffer testBuffer(outputSchema);
            auto view = testBuffer.open(buffer);
            for (size_t index = 0; index < view.getNumberOfTuples(); ++index)
            {
                Row row;
                for (const auto& name : inputNames)
                {
                    row[name] = view[index][name].as<std::string>();
                }
                row["SENTIMENT"] = view[index]["SENTIMENT"].as<std::string>();
                result.push_back(std::move(row));
            }
        }
        return result;
    }

    TestSchema inputSchema;
    TestSchema outputSchema;
    std::vector<std::string> inputNames;
    std::shared_ptr<BufferManager> inputBufferManager = createBufferManager();
    std::shared_ptr<Pipeline> pipeline;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
};

}

class SemanticMapPhysicalOperatorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SemanticMapPhysicalOperatorTest.log", LogLevel::LOG_DEBUG); }
};

/// Every record comes out exactly once, with its pass-through field untouched and its own answer.
TEST_F(SemanticMapPhysicalOperatorTest, EchoesEveryRecord)
{
    const auto config = modelConfig("echo");
    for (const bool compiled : {false, true})
    {
        SemanticMapPipeline pipeline{realFactory(config), config, {"DESCRIPTION"}};
        const auto rows = pipeline.run({{"a great film"}, {"dull"}, {"café"}, {""}, {"row with \"quotes\""}}, compiled);
        ASSERT_EQ(rows.size(), 5) << "compiled=" << compiled;
        std::unordered_map<std::string, std::string> answers;
        for (const auto& row : rows)
        {
            answers[row.at("DESCRIPTION")] = row.at("SENTIMENT");
        }
        EXPECT_EQ(answers.at("a great film"), "A GREAT FILM");
        EXPECT_EQ(answers.at("dull"), "DULL");
        EXPECT_EQ(answers.at("café"), "CAFé");
        EXPECT_EQ(answers.at(""), "");
        EXPECT_EQ(answers.at("row with \"quotes\""), "ROW WITH \"QUOTES\"");
    }
}

TEST_F(SemanticMapPhysicalOperatorTest, NormalizesAgainstDeclaredValues)
{
    const auto config = modelConfig("echo", {"POSITIVE", "NEGATIVE"}, "NEUTRAL");
    SemanticMapPipeline pipeline{realFactory(config), config, {"DESCRIPTION"}};
    const auto rows = pipeline.run({{"positive"}, {"NEGATIVE (sure)"}, {"quite negative"}, {"pozitive"}}, true);
    ASSERT_EQ(rows.size(), 4);
    std::unordered_map<std::string, std::string> answers;
    for (const auto& row : rows)
    {
        answers[row.at("DESCRIPTION")] = row.at("SENTIMENT");
    }
    EXPECT_EQ(answers.at("positive"), "POSITIVE");
    EXPECT_EQ(answers.at("NEGATIVE (sure)"), "NEGATIVE");
    EXPECT_EQ(answers.at("quite negative"), "NEGATIVE");
    EXPECT_EQ(answers.at("pozitive"), "NEUTRAL");
}

TEST_F(SemanticMapPhysicalOperatorTest, ZeroRecordBufferEmitsNothing)
{
    const auto config = modelConfig("echo");
    for (const bool compiled : {false, true})
    {
        SemanticMapPipeline pipeline{realFactory(config), config, {"DESCRIPTION"}};
        EXPECT_TRUE(pipeline.run({}, compiled).empty()) << "compiled=" << compiled;
    }
}

/// Unusable answers are the record's problem, not the query's: the default value is written and no
/// record is dropped.
TEST_F(SemanticMapPhysicalOperatorTest, UnusableResponsesWriteTheDefault)
{
    const auto config = modelConfig("unparseable", {}, "n/a");
    for (const bool compiled : {false, true})
    {
        SemanticMapPipeline unparseable{realFactory(config), config, {"DESCRIPTION"}};
        const auto rows = unparseable.run({{"a"}, {"b"}}, compiled);
        ASSERT_EQ(rows.size(), 2);
        EXPECT_EQ(rows[0].at("SENTIMENT"), "n/a");
        EXPECT_EQ(rows[1].at("SENTIMENT"), "n/a");

        SemanticMapPipeline malformed{[] { return std::make_unique<MalformedBackend>(); }, config, {"DESCRIPTION"}};
        const auto malformedRows = malformed.run({{"a"}}, compiled);
        ASSERT_EQ(malformedRows.size(), 1);
        EXPECT_EQ(malformedRows[0].at("SENTIMENT"), "n/a");
    }
}

/// An unreachable endpoint fails the query instead of silently default-filling it.
TEST_F(SemanticMapPhysicalOperatorTest, TransportFailureThrows)
{
    const auto config = modelConfig("fail");
    for (const bool compiled : {false, true})
    {
        SemanticMapPipeline pipeline{realFactory(config), config, {"DESCRIPTION"}};
        ASSERT_EXCEPTION_ERRORCODE((void)pipeline.run({{"a"}}, compiled), ErrorCode::InferenceRuntimeFailure);
    }
}

/// SPACE_JOINED joins all input fields in declared order, as the reference's `" ".join(...)`.
TEST_F(SemanticMapPhysicalOperatorTest, JoinsMultipleInputFields)
{
    const auto config = modelConfig("echo");
    for (const bool compiled : {false, true})
    {
        SemanticMapPipeline pipeline{realFactory(config), config, {"TITLE", "BODY"}};
        const auto rows = pipeline.run({{"a", "b"}}, compiled);
        ASSERT_EQ(rows.size(), 1);
        EXPECT_EQ(rows[0].at("SENTIMENT"), "A B");
    }
}

TEST_F(SemanticMapPhysicalOperatorTest, JsonObjectPayload)
{
    auto config = modelConfig("echo");
    config.payloadFormat = PayloadFormat::JSON_OBJECT;
    SemanticMapPipeline pipeline{realFactory(config), config, {"TITLE", "BODY"}};
    const auto rows = pipeline.run({{"x", "y"}}, true);
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0].at("SENTIMENT"), "X Y");
}

/// Worker threads share one compiled pipeline; each must be served its own backend instance.
TEST_F(SemanticMapPhysicalOperatorTest, EveryWorkerThreadHasItsOwnBackend)
{
    constexpr size_t NumberOfThreads = 8;
    constexpr size_t BuffersPerThread = 50;
    CountingBackend::nextId = 0;
    const auto config = modelConfig("echo");
    SemanticMapPipeline pipeline{[] { return std::make_unique<CountingBackend>(); }, config, {"DESCRIPTION"}};
    CompiledExecutablePipelineStage stage(pipeline.pipeline, pipeline.handlers, engineOptions(true));

    folly::Synchronized<std::vector<TupleBuffer>> emitted;
    auto bufferManager = createBufferManager(10 * NumberOfThreads * BuffersPerThread * 4 * BufferSize);
    pipeline.inputBufferManager = createBufferManager(10 * NumberOfThreads * BuffersPerThread * 4 * BufferSize);
    {
        MockedPipelineContext pec{emitted, bufferManager, INITIAL<WorkerThreadId>, NumberOfThreads};
        stage.start(pec);
    }

    std::vector<std::vector<TupleBuffer>> inputs(NumberOfThreads);
    for (size_t thread = 0; thread < NumberOfThreads; ++thread)
    {
        for (size_t buffer = 0; buffer < BuffersPerThread; ++buffer)
        {
            inputs[thread].push_back(pipeline.inputBuffer({{"row"}}, SequenceNumber((thread * BuffersPerThread) + buffer + 1)));
        }
    }

    std::barrier<> start(static_cast<std::ptrdiff_t>(NumberOfThreads) + 1);
    {
        std::vector<std::jthread> threads;
        for (size_t thread = 0; thread < NumberOfThreads; ++thread)
        {
            threads.emplace_back(
                [&, thread]
                {
                    MockedPipelineContext pec{emitted, bufferManager, WorkerThreadId(thread), NumberOfThreads};
                    start.arrive_and_wait();
                    for (auto& input : inputs[thread])
                    {
                        stage.execute(input, pec);
                    }
                });
        }
        start.arrive_and_wait();
    }
    {
        MockedPipelineContext pec{emitted, bufferManager, INITIAL<WorkerThreadId>, NumberOfThreads};
        stage.stop(pec);
    }

    std::unordered_map<size_t, std::unordered_set<std::string>> answersPerThread;
    size_t total = 0;
    for (const auto& buffer : *emitted.rlock())
    {
        const auto thread = (buffer.getSequenceNumber().getRawValue() - 1) / BuffersPerThread;
        for (const auto& row : pipeline.records({buffer}))
        {
            answersPerThread[thread].insert(row.at("SENTIMENT"));
            ++total;
        }
    }
    EXPECT_EQ(total, NumberOfThreads * BuffersPerThread);
    ASSERT_EQ(answersPerThread.size(), NumberOfThreads);
    std::unordered_set<std::string> backendIds;
    for (const auto& [thread, answers] : answersPerThread)
    {
        EXPECT_EQ(answers.size(), 1) << "thread " << thread << " used more than one backend";
        backendIds.insert(*answers.begin());
    }
    EXPECT_EQ(backendIds.size(), NumberOfThreads) << "two threads shared a backend";
}

/// MAX_CONCURRENCY caps the requests in flight across all worker threads of the operator.
TEST_F(SemanticMapPhysicalOperatorTest, MaxConcurrencyCapsRequestsInFlight)
{
    constexpr size_t NumberOfThreads = 4;
    constexpr size_t RecordsPerThread = 5;
    for (const size_t maxConcurrency : {size_t{1}, size_t{2}})
    {
        SlowBackend::inFlight = 0;
        SlowBackend::maxInFlight = 0;
        auto config = modelConfig("echo");
        config.maxConcurrency = maxConcurrency;
        SemanticMapPipeline pipeline{[] { return std::make_unique<SlowBackend>(); }, config, {"DESCRIPTION"}};
        CompiledExecutablePipelineStage stage(pipeline.pipeline, pipeline.handlers, engineOptions(true));

        folly::Synchronized<std::vector<TupleBuffer>> emitted;
        auto bufferManager = createBufferManager();
        {
            MockedPipelineContext pec{emitted, bufferManager, INITIAL<WorkerThreadId>, NumberOfThreads};
            stage.start(pec);
        }
        std::barrier<> start(static_cast<std::ptrdiff_t>(NumberOfThreads) + 1);
        {
            std::vector<std::jthread> threads;
            for (size_t thread = 0; thread < NumberOfThreads; ++thread)
            {
                threads.emplace_back(
                    [&, thread]
                    {
                        MockedPipelineContext pec{emitted, bufferManager, WorkerThreadId(thread), NumberOfThreads};
                        auto input = pipeline.inputBuffer(
                            std::vector<std::vector<std::string>>(RecordsPerThread, std::vector<std::string>{"row"}),
                            SequenceNumber(thread + 1));
                        start.arrive_and_wait();
                        stage.execute(input, pec);
                    });
            }
            start.arrive_and_wait();
        }
        {
            MockedPipelineContext pec{emitted, bufferManager, INITIAL<WorkerThreadId>, NumberOfThreads};
            stage.stop(pec);
        }

        size_t total = 0;
        for (const auto& buffer : *emitted.rlock())
        {
            total += pipeline.records({buffer}).size();
        }
        EXPECT_EQ(total, NumberOfThreads * RecordsPerThread);
        EXPECT_EQ(SlowBackend::maxInFlight.load(), static_cast<int>(maxConcurrency)) << "maxConcurrency=" << maxConcurrency;
    }
}

/// NOLINTEND(readability-magic-numbers, bugprone-unchecked-optional-access)

}
