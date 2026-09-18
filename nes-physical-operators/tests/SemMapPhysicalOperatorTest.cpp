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

#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <LlmClient.hpp>
#include <PhysicalOperator.hpp>
#include <ScanPhysicalOperator.hpp>
#include <SemMapPhysicalOperator.hpp>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cctype>
#include <cstddef>
#include <cstdint>
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
#include <folly/Synchronized.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <Pipeline.hpp>
#include <PipelineExecutionContext.hpp>
#include <TestTupleBuffer.hpp>
#include <options.hpp>

namespace NES
{

constexpr size_t bufferSize = 8192;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 200;
constexpr NES::BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.9;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * bufferSize;

namespace
{

/// Always answers "POSITIVE" with confidence 0.9 — used to prove confidence is discarded (D5).
struct StubLlmClient final : LlmClient
{
    SemanticMapResult map(std::string_view) override { return {{"SENTIMENT", SemanticFieldResult{.answer = "POSITIVE", .confidence = 0.9}}}; }
};

/// Echoes the uppercased input back as the answer — used to prove per-record correctness (no caching).
struct EchoLlmClient final : LlmClient
{
    SemanticMapResult map(std::string_view input) override
    {
        std::string upper{input};
        std::ranges::transform(upper, upper.begin(), [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
        return {{"SENTIMENT", SemanticFieldResult{.answer = std::move(upper), .confidence = 1.0}}};
    }
};

/// Every constructed instance gets a unique id, embedded in every answer it produces. Used to
/// prove that each worker thread got its own client instance out of the per-thread pool.
struct CountingLlmClient final : LlmClient
{
    static inline std::atomic<int> nextId{0};
    int id = nextId.fetch_add(1);

    SemanticMapResult map(std::string_view) override
    {
        return {{"SENTIMENT", SemanticFieldResult{.answer = std::to_string(id), .confidence = 1.0}}};
    }
};

}

class SemMapPhysicalOperatorTest : public Testing::BaseUnitTest
{
    /// NOLINTBEGIN(readability-magic-numbers, readability-identifier-length, bugprone-unchecked-optional-access, fuchsia-default-arguments-declarations)
protected:
    struct MockedPipelineContext final : PipelineExecutionContext
    {
        bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
        {
            buffers.wlock()->emplace_back(buffer);
            return true;
        }

        TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

        [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return threadId; }

        [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return numWorkerThreads; }

        [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

        [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override
        {
            return *operatorHandlers;
        }

        void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& opHandlers) override
        {
            operatorHandlers = &opHandlers;
        }

        void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { INVARIANT(false, "This function should not be called"); }

        ///NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) lifetime is ensured by the fixture
        folly::Synchronized<std::vector<TupleBuffer>>& buffers;
        std::shared_ptr<BufferManager> bufferManager;
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* operatorHandlers = nullptr;
        WorkerThreadId threadId = INITIAL<WorkerThreadId>;
        uint64_t numWorkerThreads = 1;

        MockedPipelineContext(
            folly::Synchronized<std::vector<TupleBuffer>>& buffers,
            std::shared_ptr<BufferManager> bufferManager,
            WorkerThreadId threadId = INITIAL<WorkerThreadId>,
            uint64_t numWorkerThreads = 1)
            : buffers(buffers), bufferManager(std::move(bufferManager)), threadId(threadId), numWorkerThreads(numWorkerThreads)
        {
        }
    };

    struct SemMapPipeline
    {
        std::shared_ptr<Pipeline> pipeline;
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
    };

public:
    static void SetUpTestSuite() { Logger::setupLogging("SemMapPhysicalOperatorTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override { BaseUnitTest::SetUp(); }

    using TestSchema = Schema<UnqualifiedUnboundField, Ordered>;

    static UnqualifiedUnboundField makeField(std::string_view name, DataType::Type type)
    {
        return UnqualifiedUnboundField{Identifier::parse(std::string{name}), DataType{type, DataType::NULLABLE::NOT_NULLABLE}};
    }

    static std::vector<QualifiedIdentifier> toIdLists(const std::vector<std::string>& names)
    {
        return names | std::views::transform([](const std::string& n) { return QualifiedIdentifier::tryParse(n).value(); })
            | std::ranges::to<std::vector>();
    }

    static std::vector<QualifiedIdentifier> toIdLists(const TestSchema& schema)
    {
        return schema
            | std::views::transform([](const UnqualifiedUnboundField& f)
                                    { return static_cast<QualifiedIdentifier>(f.getFullyQualifiedName()); })
            | std::ranges::to<std::vector>();
    }

    /// Input: {description VARSIZED}. Output: {description VARSIZED, <outputFieldNames> VARSIZED...}.
    static std::pair<TestSchema, TestSchema> makeSchemas(const std::vector<std::string>& outputFieldNames)
    {
        auto inputSchema = std::vector{makeField("description", DataType::Type::VARSIZED)} | std::ranges::to<TestSchema>();

        std::vector<UnqualifiedUnboundField> outputFields;
        outputFields.push_back(makeField("description", DataType::Type::VARSIZED));
        for (const auto& name : outputFieldNames)
        {
            outputFields.push_back(makeField(name, DataType::Type::VARSIZED));
        }
        auto outputSchema = std::move(outputFields) | std::ranges::to<TestSchema>();
        return {std::move(inputSchema), std::move(outputSchema)};
    }

    /// Creates a Scan -> SemMap -> Emit pipeline. `modelOutputNames` are the catalog OUTPUT names
    /// the LLM result map is keyed by; `outputFieldNames` are the record fields written — they
    /// diverge when a call-site alias renames the model output (plan §M4, Step 1).
    static SemMapPipeline createSemMapPipeline(
        LlmClientFactory factory,
        const TestSchema& inputSchema,
        const TestSchema& outputSchema,
        const std::vector<std::string>& inputFieldNames,
        const std::vector<std::string>& outputFieldNames,
        const std::vector<std::string>& modelOutputNames)
    {
        auto inputBufRef = LowerSchemaProvider::lowerSchema(bufferSize, inputSchema, MemoryLayoutType::ROW_LAYOUT);
        auto outputBufRef = LowerSchemaProvider::lowerSchema(bufferSize, outputSchema, MemoryLayoutType::ROW_LAYOUT);

        ScanPhysicalOperator scan(inputBufRef, toIdLists(inputSchema));
        SemMapPhysicalOperator semMap(
            std::move(factory), toIdLists(inputFieldNames), toIdLists(outputFieldNames), modelOutputNames);
        const EmitPhysicalOperator emit(OperatorHandlerId(1), outputBufRef);

        semMap.setChild(PhysicalOperator(emit));
        scan.setChild(PhysicalOperator(semMap));

        auto pipeline = std::make_shared<Pipeline>(PhysicalOperator(scan));

        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
        handlers[OperatorHandlerId(1)] = std::make_shared<EmitOperatorHandler>();

        return {.pipeline = std::move(pipeline), .handlers = std::move(handlers)};
    }

    static TupleBuffer createInputBuffer(const TestSchema& inputSchema, const std::vector<std::string>& descriptions)
    {
        auto bufMgr = BufferManager::create(
            TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, bufferSize, std::make_shared<NesDefaultMemoryAllocator>());
        auto tupleBuffer = bufMgr->getBufferBlocking();
        tupleBuffer.setSequenceNumber(SequenceNumber(1));
        tupleBuffer.setChunkNumber(ChunkNumber(1));
        tupleBuffer.setLastChunk(true);
        tupleBuffer.setOriginId(INITIAL<OriginId>);

        Testing::TestTupleBuffer ttb(inputSchema);
        auto view = ttb.open(tupleBuffer, bufMgr.get());
        for (const auto& description : descriptions)
        {
            view.append(description);
        }
        return tupleBuffer;
    }

    static nautilus::engine::Options makeEngineOptions(bool compiled)
    {
        nautilus::engine::Options opt;
        opt.setOption("engine.Compilation", compiled);
        opt.setOption("engine.backend", std::string("mlir"));
        opt.setOption("engine.compilationStrategy", std::string("legacy"));
        return opt;
    }
};

/// One record in, one record out carrying the stub's answer. Interpreted + compiled.
TEST_F(SemMapPhysicalOperatorTest, SingleRecord)
{
    const auto [inputSchema, outputSchema] = makeSchemas({"SENTIMENT"});
    auto inputBuffer = createInputBuffer(inputSchema, {"a great product"});

    for (bool compiled : {false, true})
    {
        auto [pipeline, handlers] = createSemMapPipeline(
            [] { return std::make_unique<StubLlmClient>(); }, inputSchema, outputSchema, {"description"}, {"sentiment"}, {"SENTIMENT"});
        CompiledExecutablePipelineStage stage(pipeline, handlers, makeEngineOptions(compiled));

        folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
        auto bufMgr = BufferManager::create(
            TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, bufferSize, std::make_shared<NesDefaultMemoryAllocator>());
        MockedPipelineContext pec{emittedBuffers, bufMgr};

        stage.start(pec);
        stage.execute(inputBuffer, pec);
        stage.stop(pec);

        size_t totalRecords = 0;
        auto lockedBuffers = *emittedBuffers.rlock();
        for (auto& outBuf : lockedBuffers)
        {
            Testing::TestTupleBuffer ttb(outputSchema);
            auto view = ttb.open(outBuf);
            for (size_t row = 0; row < view.getNumberOfTuples(); ++row)
            {
                EXPECT_EQ(view[row]["sentiment"].as<std::string>(), "POSITIVE") << "(compiled=" << compiled << ")";
                ++totalRecords;
            }
        }
        EXPECT_EQ(totalRecords, 1U) << "(compiled=" << compiled << ")";
    }
}

/// 5 records, echoing stub distinguishes per-row answers — proves nothing is cached across records.
TEST_F(SemMapPhysicalOperatorTest, MultiRecord)
{
    constexpr size_t numRecords = 5;
    const auto [inputSchema, outputSchema] = makeSchemas({"SENTIMENT"});
    std::vector<std::string> inputs;
    for (size_t i = 0; i < numRecords; ++i)
    {
        inputs.push_back("row" + std::to_string(i));
    }
    auto inputBuffer = createInputBuffer(inputSchema, inputs);

    for (bool compiled : {false, true})
    {
        auto [pipeline, handlers] = createSemMapPipeline(
            [] { return std::make_unique<EchoLlmClient>(); }, inputSchema, outputSchema, {"description"}, {"sentiment"}, {"SENTIMENT"});
        CompiledExecutablePipelineStage stage(pipeline, handlers, makeEngineOptions(compiled));

        folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
        auto bufMgr = BufferManager::create(
            TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, bufferSize, std::make_shared<NesDefaultMemoryAllocator>());
        MockedPipelineContext pec{emittedBuffers, bufMgr};

        stage.start(pec);
        stage.execute(inputBuffer, pec);
        stage.stop(pec);

        std::unordered_set<std::string> seenAnswers;
        size_t totalRecords = 0;
        auto lockedBuffers = *emittedBuffers.rlock();
        for (auto& outBuf : lockedBuffers)
        {
            Testing::TestTupleBuffer ttb(outputSchema);
            auto view = ttb.open(outBuf);
            for (size_t row = 0; row < view.getNumberOfTuples(); ++row)
            {
                const auto description = view[row]["description"].as<std::string>();
                std::string expected = description;
                std::ranges::transform(expected, expected.begin(), [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
                EXPECT_EQ(view[row]["sentiment"].as<std::string>(), expected) << "(compiled=" << compiled << ")";
                seenAnswers.insert(view[row]["sentiment"].as<std::string>());
                ++totalRecords;
            }
        }
        EXPECT_EQ(totalRecords, numRecords) << "(compiled=" << compiled << ")";
        EXPECT_EQ(seenAnswers.size(), numRecords) << "Expected distinct per-record answers (compiled=" << compiled << ")";
    }
}

/// Zero-record buffer produces zero output records. Interpreted + compiled.
TEST_F(SemMapPhysicalOperatorTest, ZeroRecordBuffer)
{
    const auto [inputSchema, outputSchema] = makeSchemas({"SENTIMENT"});
    auto inputBuffer = createInputBuffer(inputSchema, {});

    for (bool compiled : {false, true})
    {
        auto [pipeline, handlers] = createSemMapPipeline(
            [] { return std::make_unique<StubLlmClient>(); }, inputSchema, outputSchema, {"description"}, {"sentiment"}, {"SENTIMENT"});
        CompiledExecutablePipelineStage stage(pipeline, handlers, makeEngineOptions(compiled));

        folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
        auto bufMgr = BufferManager::create(
            TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, bufferSize, std::make_shared<NesDefaultMemoryAllocator>());
        MockedPipelineContext pec{emittedBuffers, bufMgr};

        stage.start(pec);
        stage.execute(inputBuffer, pec);
        stage.stop(pec);

        size_t totalRecords = 0;
        auto lockedBuffers = *emittedBuffers.rlock();
        for (auto& outBuf : lockedBuffers)
        {
            Testing::TestTupleBuffer ttb(outputSchema);
            auto view = ttb.open(outBuf);
            totalRecords += view.getNumberOfTuples();
        }
        EXPECT_EQ(totalRecords, 0U) << "(compiled=" << compiled << ")";
    }
}

/// 8 threads, 50 buffers each, shared compiled pipeline; a counting stub proves each worker
/// thread was served its own client out of the per-thread pool.
TEST_F(SemMapPhysicalOperatorTest, ConcurrentStress)
{
    constexpr size_t numThreads = 8;
    constexpr size_t buffersPerThread = 50;
    const auto [inputSchema, outputSchema] = makeSchemas({"SENTIMENT"});

    CountingLlmClient::nextId = 0;
    auto [pipeline, handlers] = createSemMapPipeline(
        [] { return std::make_unique<CountingLlmClient>(); }, inputSchema, outputSchema, {"description"}, {"sentiment"}, {"SENTIMENT"});

    CompiledExecutablePipelineStage stage(pipeline, handlers, makeEngineOptions(true));

    folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
    auto bufMgr = BufferManager::create(
        10 * static_cast<size_t>(numThreads) * buffersPerThread * 4 * bufferSize,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        bufferSize,
        std::make_shared<NesDefaultMemoryAllocator>());

    {
        MockedPipelineContext startPec{emittedBuffers, bufMgr, INITIAL<WorkerThreadId>, static_cast<uint64_t>(numThreads)};
        stage.start(startPec);
    }

    std::vector<std::vector<TupleBuffer>> threadInputBuffers(numThreads);
    for (size_t tid = 0; tid < numThreads; ++tid)
    {
        threadInputBuffers[tid].reserve(buffersPerThread);
        for (size_t buf = 0; buf < buffersPerThread; ++buf)
        {
            auto inputBuf = createInputBuffer(inputSchema, {"row"});
            inputBuf.setSequenceNumber(SequenceNumber((tid * buffersPerThread) + buf + 1));
            inputBuf.setChunkNumber(INITIAL_CHUNK_NUMBER);
            inputBuf.setLastChunk(true);
            inputBuf.setOriginId(INITIAL<OriginId>);
            threadInputBuffers[tid].push_back(std::move(inputBuf));
        }
    }

    std::barrier<> startBarrier(static_cast<int>(numThreads) + 1);
    std::vector<std::jthread> threads;
    threads.reserve(numThreads);

    for (size_t tid = 0; tid < numThreads; ++tid)
    {
        threads.emplace_back(
            [tid, &threadInputBuffers, &stage, &emittedBuffers, &bufMgr, &startBarrier]()
            {
                MockedPipelineContext threadPec{emittedBuffers, bufMgr, WorkerThreadId(tid), static_cast<uint64_t>(numThreads)};
                startBarrier.arrive_and_wait();
                for (auto& inputBuf : threadInputBuffers[tid])
                {
                    stage.execute(inputBuf, threadPec);
                }
            });
    }

    startBarrier.arrive_and_wait();
    threads.clear();

    {
        MockedPipelineContext stopPec{emittedBuffers, bufMgr, INITIAL<WorkerThreadId>, static_cast<uint64_t>(numThreads)};
        stage.stop(stopPec);
    }

    /// sequence number => (tid*buffersPerThread)+buf+1 lets us recover the originating thread
    /// without relying on emit order.
    std::unordered_map<size_t, std::unordered_set<std::string>> answersPerThread;
    size_t totalRecords = 0;

    auto lockedBuffers = *emittedBuffers.rlock();
        for (auto& outBuf : lockedBuffers)
    {
        const auto seqNum = outBuf.getSequenceNumber().getRawValue();
        const auto tid = (seqNum - 1) / buffersPerThread;

        Testing::TestTupleBuffer ttb(outputSchema);
        auto view = ttb.open(outBuf);
        for (size_t row = 0; row < view.getNumberOfTuples(); ++row)
        {
            answersPerThread[tid].insert(view[row]["sentiment"].as<std::string>());
            ++totalRecords;
        }
    }

    EXPECT_EQ(totalRecords, numThreads * buffersPerThread);
    EXPECT_EQ(answersPerThread.size(), numThreads) << "Expected every thread to have produced output";
    for (const auto& [tid, answers] : answersPerThread)
    {
        EXPECT_EQ(answers.size(), 1U) << "Thread " << tid << " should have used exactly one client throughout";
    }

    std::unordered_set<std::string> allIds;
    for (const auto& [tid, answers] : answersPerThread)
    {
        allIds.insert(*answers.begin());
    }
    EXPECT_EQ(allIds.size(), numThreads) << "Every thread should have been served a distinct client from the per-thread pool";
}

/// Confidence is parsed by the client but discarded by the physical operator (D5, plan §2.1):
/// only the answer reaches the output schema/record.
TEST_F(SemMapPhysicalOperatorTest, ConfidenceDiscarded)
{
    const auto [inputSchema, outputSchema] = makeSchemas({"SENTIMENT"});
    EXPECT_FALSE(outputSchema[Identifier::parse("sentiment_confidence")].has_value());

    auto inputBuffer = createInputBuffer(inputSchema, {"a great product"});

    auto [pipeline, handlers] = createSemMapPipeline(
        [] { return std::make_unique<StubLlmClient>(); }, inputSchema, outputSchema, {"description"}, {"sentiment"}, {"SENTIMENT"});
    CompiledExecutablePipelineStage stage(pipeline, handlers, makeEngineOptions(true));

    folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
    auto bufMgr = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, bufferSize, std::make_shared<NesDefaultMemoryAllocator>());
    MockedPipelineContext pec{emittedBuffers, bufMgr};

    stage.start(pec);
    stage.execute(inputBuffer, pec);
    stage.stop(pec);

    size_t totalRecords = 0;
    auto lockedBuffers = *emittedBuffers.rlock();
        for (auto& outBuf : lockedBuffers)
    {
        Testing::TestTupleBuffer ttb(outputSchema);
        auto view = ttb.open(outBuf);
        for (size_t row = 0; row < view.getNumberOfTuples(); ++row)
        {
            EXPECT_EQ(view[row]["sentiment"].as<std::string>(), "POSITIVE");
            ++totalRecords;
        }
    }
    EXPECT_EQ(totalRecords, 1U);
}

/// The call-site alias renames the model output: the record field is the alias while the result
/// map stays keyed by the catalog OUTPUT name (plan §M4, Step 1 — before the split, the operator
/// looked the answer up under the alias and silently wrote nothing).
TEST_F(SemMapPhysicalOperatorTest, AliasDiffersFromModelOutputName)
{
    const auto [inputSchema, outputSchema] = makeSchemas({"mood"});
    auto inputBuffer = createInputBuffer(inputSchema, {"a great product"});

    for (bool compiled : {false, true})
    {
        auto [pipeline, handlers] = createSemMapPipeline(
            [] { return std::make_unique<StubLlmClient>(); }, inputSchema, outputSchema, {"description"}, {"mood"}, {"SENTIMENT"});
        CompiledExecutablePipelineStage stage(pipeline, handlers, makeEngineOptions(compiled));

        folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
        auto bufMgr = BufferManager::create(
            TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, bufferSize, std::make_shared<NesDefaultMemoryAllocator>());
        MockedPipelineContext pec{emittedBuffers, bufMgr};

        stage.start(pec);
        stage.execute(inputBuffer, pec);
        stage.stop(pec);

        size_t totalRecords = 0;
        auto lockedBuffers = *emittedBuffers.rlock();
        for (auto& outBuf : lockedBuffers)
        {
            Testing::TestTupleBuffer ttb(outputSchema);
            auto view = ttb.open(outBuf);
            for (size_t row = 0; row < view.getNumberOfTuples(); ++row)
            {
                EXPECT_EQ(view[row]["mood"].as<std::string>(), "POSITIVE") << "(compiled=" << compiled << ")";
                ++totalRecords;
            }
        }
        EXPECT_EQ(totalRecords, 1U) << "(compiled=" << compiled << ")";
    }
}

/// Two VARSIZED input columns must be space-joined into a single call (plan §M4, Step 2a):
/// LlmClient.hpp documents the argument as "space-joined INPUT field values", but execute() used
/// to read only inputFieldNames.at(0), silently dropping every column past the first.
TEST_F(SemMapPhysicalOperatorTest, JoinsMultipleInputFields)
{
    auto inputSchema = std::vector{makeField("a", DataType::Type::VARSIZED), makeField("b", DataType::Type::VARSIZED)}
        | std::ranges::to<TestSchema>();
    auto outputSchema
        = std::vector{
              makeField("a", DataType::Type::VARSIZED), makeField("b", DataType::Type::VARSIZED),
              makeField("sentiment", DataType::Type::VARSIZED)}
        | std::ranges::to<TestSchema>();

    auto bufMgr = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, bufferSize, std::make_shared<NesDefaultMemoryAllocator>());
    auto tupleBuffer = bufMgr->getBufferBlocking();
    tupleBuffer.setSequenceNumber(SequenceNumber(1));
    tupleBuffer.setChunkNumber(ChunkNumber(1));
    tupleBuffer.setLastChunk(true);
    tupleBuffer.setOriginId(INITIAL<OriginId>);
    Testing::TestTupleBuffer ttb(inputSchema);
    auto view = ttb.open(tupleBuffer, bufMgr.get());
    view.append("a", "b");
    auto inputBuffer = tupleBuffer;

    for (bool compiled : {false, true})
    {
        auto [pipeline, handlers] = createSemMapPipeline(
            [] { return std::make_unique<EchoLlmClient>(); }, inputSchema, outputSchema, {"a", "b"}, {"sentiment"}, {"SENTIMENT"});
        CompiledExecutablePipelineStage stage(pipeline, handlers, makeEngineOptions(compiled));

        folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
        auto stageBufMgr = BufferManager::create(
            TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, bufferSize, std::make_shared<NesDefaultMemoryAllocator>());
        MockedPipelineContext pec{emittedBuffers, stageBufMgr};

        stage.start(pec);
        stage.execute(inputBuffer, pec);
        stage.stop(pec);

        size_t totalRecords = 0;
        auto lockedBuffers = *emittedBuffers.rlock();
        for (auto& outBuf : lockedBuffers)
        {
            Testing::TestTupleBuffer outView(outputSchema);
            auto readView = outView.open(outBuf);
            for (size_t row = 0; row < readView.getNumberOfTuples(); ++row)
            {
                EXPECT_EQ(readView[row]["sentiment"].as<std::string>(), "A B") << "(compiled=" << compiled << ")";
                ++totalRecords;
            }
        }
        EXPECT_EQ(totalRecords, 1U) << "(compiled=" << compiled << ")";
    }
}

/// NOLINTEND(readability-magic-numbers, readability-identifier-length, bugprone-unchecked-optional-access, fuchsia-default-arguments-declarations)

}
