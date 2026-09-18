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

#include <algorithm>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <memory>
#include <random>
#include <ranges>
#include <set>
#include <source_location>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <gtest/gtest.h>
#include <nautilus/Engine.hpp>
#include <nautilus/RuntimeBinding.hpp>
#include <nautilus/val_details.hpp>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <BaseUnitTest.hpp>
#include <CompilationContext.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <Pipeline.hpp>
#include <PipelineExecutionContext.hpp>
#include <ScanPhysicalOperator.hpp>

namespace NES
{

namespace
{
constexpr uint32_t POOLED_BUFFER_SIZE = 512;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 100000;
constexpr NES::BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.9;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;

nautilus::engine::Options compilerOptions()
{
    nautilus::engine::Options options;
    options.setOption("engine.backend", std::string("mlir"));
    options.setOption("engine.compilationStrategy", std::string("legacy"));
    return options;
}
}

class EmitPhysicalOperatorTest : public Testing::BaseUnitTest
{
public:
    struct MockedPipelineContext final : PipelineExecutionContext
    {
        bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
        {
            buffers.wlock()->emplace_back(buffer);
            return true;
        }

        TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

        [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return INITIAL<WorkerThreadId>; }

        [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

        [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

        [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

        MockedPipelineContext(folly::Synchronized<std::vector<TupleBuffer>>& buffers, std::shared_ptr<BufferManager> bufferManager)
            : buffers(buffers), bufferManager(std::move(bufferManager))
        {
        }

        void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { INVARIANT(false, "This function should not be called"); }

        ///NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) lifetime is ensured by the `run` method.
        folly::Synchronized<std::vector<TupleBuffer>>& buffers;
        std::shared_ptr<BufferManager> bufferManager;
    };

    static void SetUpTestSuite()
    {
        Logger::setupLogging("EmitPhysicalOperatorTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup EmitPhysicalOperatorTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        reset();
    }

    EmitPhysicalOperator createUUT()
    {
        auto schema = Schema<QualifiedUnboundField, Ordered>{QualifiedUnboundField{Identifier::parse("A_FIELD"), DataType::Type::UINT32}};
        auto bufferRef = LowerSchemaProvider::lowerSchema(512, schema, MemoryLayoutType::ROW_LAYOUT);
        EmitPhysicalOperator emit{OperatorHandlerId(0), std::move(bufferRef)};
        handlers.insert_or_assign(OperatorHandlerId(0), std::make_shared<EmitOperatorHandler>());
        return emit;
    }

    void run(const std::function<void(ExecutionContext&, RecordBuffer&)>& test, TupleBuffer buffer)
    {
        MockedPipelineContext pec{buffers, bm};
        nautilus::RuntimeBindings bindings;
        OperatorHandlerBindings handlerBindings;
        CompilationContext compilationContext{pec, handlers, bindings, handlerBindings};
        for (const auto& [id, handler] : handlers)
        {
            compilationContext.registerOperatorHandler(id);
        }
        Arena arena(bm);

        ExecutionContext executionContext{&pec, &arena, &handlerBindings};
        executionContext.chunkNumber = buffer.getChunkNumber();
        executionContext.sequenceNumber = buffer.getSequenceNumber(), executionContext.lastChunk = buffer.isLastChunk();
        executionContext.originId = buffer.getOriginId();

        RecordBuffer recordBuffer{BorrowedNautilusBuffer::from(std::addressof(buffer))};
        test(executionContext, recordBuffer);
    }

    ///NOLINTBEGIN(fuchsia-default-arguments-declarations)

    void checkNumberOfBuffers(size_t numberOfBuffers, std::source_location location = std::source_location::current())
    {
        const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "checkNumberOfBuffers");
        EXPECT_EQ(buffers.rlock()->size(), numberOfBuffers) << fmt::format("expects {} buffers to be emitted", numberOfBuffers);
    }

    void checkBufferAt(
        size_t index,
        SequenceNumber::Underlying sequence,
        ChunkNumber::Underlying chunkNumber,
        bool isLastChunk = false,
        OriginId originId = INITIAL<OriginId>,
        size_t numberOfTuples = 0,
        std::source_location location = std::source_location::current())
    {
        const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "checkBufferAt");
        ASSERT_GE(buffers.rlock()->size(), index) << fmt::format("Index out of bound when checking buffer at {}", index);
        EXPECT_EQ(buffers.rlock()->at(index).getNumberOfTuples(), numberOfTuples) << fmt::format("Expected {} tuples", numberOfTuples);
        EXPECT_EQ(buffers.rlock()->at(index).getSequenceNumber(), SequenceNumber(sequence))
            << fmt::format("Expected Sequence Number {}", sequence);
        EXPECT_EQ(buffers.rlock()->at(index).getChunkNumber(), ChunkNumber(chunkNumber))
            << fmt::format("Expected Chunk Number {}", sequence);
        EXPECT_EQ(buffers.rlock()->at(index).getOriginId(), OriginId(originId)) << fmt::format("Expected Chunk Number {}", sequence);
        EXPECT_EQ(buffers.rlock()->at(index).isLastChunk(), isLastChunk);
    }

    void checkForDups(std::source_location location = std::source_location::current())
    {
        const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "checkForDups");
        auto uniqueSequences = (*buffers.rlock())
            | std::views::transform([](const auto& buffer)
                                    { return SequenceData(buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk()); })
            | std::ranges::to<std::set>();

        EXPECT_EQ(buffers.rlock()->size(), uniqueSequences.size()) << "Received duplicate sequences";
    }

    void checkLastChunks(std::source_location location = std::source_location::current())
    {
        const testing::ScopedTrace scopedTrace(location.file_name(), static_cast<int>(location.line()), "checkForLastChunks");
        std::unordered_map<SequenceNumber, std::tuple<size_t, ChunkNumber::Underlying, bool, ChunkNumber::Underlying>>
            sequenceNumberTerminations;

        for (const auto& buffer : *buffers.rlock())
        {
            auto& [seen, maxChunkNumber, termination, terminationAt] = sequenceNumberTerminations[buffer.getSequenceNumber()];
            seen++;
            maxChunkNumber = std::max(maxChunkNumber, buffer.getChunkNumber().getRawValue());
            if (buffer.isLastChunk())
            {
                terminationAt = buffer.getChunkNumber().getRawValue();
            }
            EXPECT_FALSE(termination && buffer.isLastChunk())
                << fmt::format("Sequence {} has multiple last chunks", buffer.getSequenceNumber());
            termination |= buffer.isLastChunk();
        }

        for (const auto& [seq, t] : sequenceNumberTerminations)
        {
            const auto& [seen, max, terminated, terminationAt] = t;
            EXPECT_TRUE(terminated) << fmt::format("Sequence {} is not terminated", seq);
            EXPECT_EQ(terminationAt, max) << fmt::format("Sequence {} Non Max Chunk Number has Last Flag", seq);
            EXPECT_EQ(seen, max) << fmt::format("Sequence {}: The maximum chunk number is {}, but we only saw {} chunks", seq, max, seen);
        }
    }

    TupleBuffer createBuffer(
        SequenceNumber::Underlying sequence,
        ChunkNumber::Underlying chunkNumber,
        bool isLastChunk = false,
        OriginId originId = INITIAL<OriginId>,
        size_t numberOfTuples = 0) const
    {
        auto buffer = bm->getBufferBlocking();
        buffer.setNumberOfTuples(numberOfTuples);
        buffer.setLastChunk(isLastChunk);
        buffer.setChunkNumber(ChunkNumber(chunkNumber));
        buffer.setSequenceNumber(SequenceNumber(sequence));
        buffer.setOriginId(originId);

        return buffer;
    }

    ///NOLINTEND(fuchsia-default-arguments-declarations)

    void reset() { buffers.wlock()->clear(); }

    folly::Synchronized<std::vector<TupleBuffer>> buffers;
    std::shared_ptr<BufferManager> bm = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;

    std::random_device rd;
};

TEST_F(EmitPhysicalOperatorTest, HandlerLookupDoesNotInsertMissingHandler)
{
    const auto handler = std::make_shared<EmitOperatorHandler>();
    handlers.emplace(OperatorHandlerId(4), handler);
    run(
        [&](ExecutionContext& ctx, RecordBuffer&)
        {
            EXPECT_EQ(
                nautilus::details::RawValueResolver<OperatorHandler*>::getRawValue(ctx.getGlobalOperatorHandler(OperatorHandlerId(4))),
                handler.get());
            EXPECT_EQ(
                nautilus::details::RawValueResolver<OperatorHandler*>::getRawValue(ctx.getGlobalOperatorHandler(OperatorHandlerId(99))),
                nullptr);
            EXPECT_EQ(handlers.size(), 1);
        },
        createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL, true));
}

TEST_F(EmitPhysicalOperatorTest, BasicTest)
{
    auto buffer = createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL, true);
    EmitPhysicalOperator emit = createUUT();

    run(
        [&](auto& executionContext, auto& recordBuffer)
        {
            emit.open(executionContext, recordBuffer);
            emit.close(executionContext, recordBuffer);
        },
        buffer);

    checkBufferAt(0, SequenceNumber::INITIAL, ChunkNumber::INITIAL, true);
    checkForDups();
    checkLastChunks();
}

TEST_F(EmitPhysicalOperatorTest, HandlerBindingsFollowSetupOrderInsteadOfGlobalIds)
{
    nautilus::engine::NautilusEngine engine(compilerOptions());
    std::string previousSchema;
    for (const auto& [firstId, secondId] : {std::pair{91ULL, 4ULL}, std::pair{2ULL, 101ULL}})
    {
        handlers.clear();
        handlers.emplace(OperatorHandlerId(firstId), std::make_shared<EmitOperatorHandler>());
        handlers.emplace(OperatorHandlerId(secondId), std::make_shared<EmitOperatorHandler>());
        MockedPipelineContext pec{buffers, bm};
        Arena arena(bm);
        OperatorHandlerBindings handlerBindings;
        ExecutionContext context{&pec, &arena, &handlerBindings};
        nautilus::RuntimeBindings bindings;
        auto module = engine.createModule();
        CompilationContext compilationContext{pec, handlers, bindings, handlerBindings};
        compilationContext.registerOperatorHandler(OperatorHandlerId(firstId));
        compilationContext.registerOperatorHandler(OperatorHandlerId(secondId));
        compilationContext.registerOperatorHandler(OperatorHandlerId(firstId));
        EXPECT_EQ(handlerBindings.size(), 2);
        EXPECT_EQ(bindings.entries().size(), 2);
        EXPECT_EQ(bindings.entries().at("handler/0")->address, handlers.at(OperatorHandlerId(firstId)).get());
        EXPECT_EQ(bindings.entries().at("handler/1")->address, handlers.at(OperatorHandlerId(secondId)).get());
        if (!previousSchema.empty())
        {
            EXPECT_EQ(bindings.schema(), previousSchema);
        }
        previousSchema = bindings.schema();
        module.registerFunction(
            "first",
            std::function<nautilus::val<OperatorHandler*>()>([&] { return context.getGlobalOperatorHandler(OperatorHandlerId(firstId)); }));
        module.registerFunction(
            "second",
            std::function<nautilus::val<OperatorHandler*>()>([&]
                                                             { return context.getGlobalOperatorHandler(OperatorHandlerId(secondId)); }));
        module.setRuntimeBindings(bindings);
        auto compiled = module.compile();
        EXPECT_EQ(compiled.getFunction<OperatorHandler*()>("first")(), handlers.at(OperatorHandlerId(firstId)).get());
        EXPECT_EQ(compiled.getFunction<OperatorHandler*()>("second")(), handlers.at(OperatorHandlerId(secondId)).get());
    }
}

TEST_F(EmitPhysicalOperatorTest, ExecutionContextBorrowsHandlerBindingsRegisteredAfterConstruction)
{
    const auto handler = std::make_shared<EmitOperatorHandler>();
    handlers.emplace(OperatorHandlerId(4), handler);
    MockedPipelineContext pec{buffers, bm};
    Arena arena(bm);
    OperatorHandlerBindings handlerBindings;
    ExecutionContext context{&pec, &arena, &handlerBindings};
    {
        nautilus::RuntimeBindings bindings;
        CompilationContext compilationContext{pec, handlers, bindings, handlerBindings};
        compilationContext.registerOperatorHandler(OperatorHandlerId(4));
    }
    EXPECT_EQ(
        nautilus::details::RawValueResolver<OperatorHandler*>::getRawValue(context.getGlobalOperatorHandler(OperatorHandlerId(4))),
        handler.get());
    EXPECT_EQ(
        nautilus::details::RawValueResolver<OperatorHandler*>::getRawValue(context.getGlobalOperatorHandler(OperatorHandlerId(99))),
        nullptr);
    EXPECT_EQ(handlerBindings.size(), 1);
    EXPECT_EQ(handlers.size(), 1);
}

TEST_F(EmitPhysicalOperatorTest, HandlerRegistrationRejectsMissingOrNullOwners)
{
    MockedPipelineContext pec{buffers, bm};
    nautilus::RuntimeBindings bindings;
    OperatorHandlerBindings handlerBindings;
    CompilationContext compilationContext{pec, handlers, bindings, handlerBindings};
    EXPECT_THROW(compilationContext.registerOperatorHandler(OperatorHandlerId(4)), std::out_of_range);
    handlers.emplace(OperatorHandlerId(4), nullptr);
    EXPECT_THROW(compilationContext.registerOperatorHandler(OperatorHandlerId(4)), std::invalid_argument);
    EXPECT_TRUE(handlerBindings.empty());
    EXPECT_TRUE(bindings.entries().empty());
}

TEST_F(EmitPhysicalOperatorTest, UnregisteredHandlerCannotBeLookedUpWhileTracing)
{
    MockedPipelineContext pec{buffers, bm};
    Arena arena(bm);
    ExecutionContext unboundContext{&pec, &arena};
    EXPECT_THROW(std::ignore = unboundContext.getGlobalOperatorHandler(OperatorHandlerId(0)), std::logic_error);
    OperatorHandlerBindings handlerBindings;
    for (const auto* bindings : std::initializer_list<const OperatorHandlerBindings*>{nullptr, &handlerBindings})
    {
        SCOPED_TRACE(bindings != nullptr);
        nautilus::engine::NautilusEngine engine(compilerOptions());
        ExecutionContext context{&pec, &arena, bindings};
        auto module = engine.createModule();
        module.registerFunction(
            "unregistered",
            std::function<nautilus::val<OperatorHandler*>()>([&] { return context.getGlobalOperatorHandler(OperatorHandlerId(0)); }));
        EXPECT_THROW(module.compile(), std::logic_error);
    }
    EXPECT_TRUE(handlerBindings.empty());
    EXPECT_TRUE(handlers.empty());
}

TEST_F(EmitPhysicalOperatorTest, PipelineUsesFreshBoundEmitHandlerOnRestart)
{
    for (const bool compilation : {true, false})
    {
        SCOPED_TRACE(compilation);
        reset();
        const auto schema
            = Schema<QualifiedUnboundField, Ordered>{QualifiedUnboundField{Identifier::parse("A_FIELD"), DataType::Type::UINT32}};
        auto bufferRef = LowerSchemaProvider::lowerSchema(POOLED_BUFFER_SIZE, schema, MemoryLayoutType::ROW_LAYOUT);
        auto emit = createUUT();
        auto root = PhysicalOperator(ScanPhysicalOperator(bufferRef, bufferRef->getAllFieldNames())).withChild(emit);
        auto pipeline = std::make_shared<Pipeline>(root);
        auto options = compilerOptions();
        options.setOption("engine.Compilation", compilation);
        const std::weak_ptr<OperatorHandler> handler = handlers.at(OperatorHandlerId(0));
        {
            CompiledExecutablePipelineStage stage(pipeline, handlers, options);
            handlers.clear();
            MockedPipelineContext pec{buffers, bm};
            for (size_t restart = 0; restart < 2; ++restart)
            {
                SCOPED_TRACE(restart);
                stage.start(pec);
                EXPECT_FALSE(handler.expired());
                auto buffer = createBuffer(SequenceNumber::INITIAL + restart, ChunkNumber::INITIAL, true);
                stage.execute(buffer, pec);
                stage.stop(pec);
                checkNumberOfBuffers(restart + 1);
                checkBufferAt(restart, SequenceNumber::INITIAL + restart, ChunkNumber::INITIAL, true);
            }
        }
        EXPECT_TRUE(handler.expired());
    }
}

TEST_F(EmitPhysicalOperatorTest, ChunkNumberTest)
{
    std::vector<TupleBuffer> inputBuffers;
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL + 1, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL + 2, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL + 3, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL + 4, true));


    bool hasMorePermutations = true;
    while (hasMorePermutations)
    {
        reset();
        EmitPhysicalOperator emit = createUUT();
        for (auto& buffer : inputBuffers)
        {
            run(
                [&](auto& executionContext, auto& recordBuffer)
                {
                    emit.open(executionContext, recordBuffer);
                    emit.close(executionContext, recordBuffer);
                },
                buffer);
        }
        checkNumberOfBuffers(5);
        checkBufferAt(4, SequenceNumber::INITIAL, ChunkNumber::INITIAL + 4, true);
        checkForDups();
        checkLastChunks();

        hasMorePermutations = std::ranges::next_permutation(
                                  inputBuffers,
                                  std::less{},
                                  [](const TupleBuffer& buffer)
                                  { return SequenceData(buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk()); })
                                  .found;
    }
}

/// Tests if all permutations result in a sane ordering of chunk numbers.
/// This means every sequence number should have the same number of chunks as inserted (albeit in different order) with exactly one chunk
/// marked as the last chunk, and no duplicates.
TEST_F(EmitPhysicalOperatorTest, SequenceChunkNumberTest)
{
    std::vector<TupleBuffer> inputBuffers;
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL + 1, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL, ChunkNumber::INITIAL + 2, true));

    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL + 1, ChunkNumber::INITIAL, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL + 1, ChunkNumber::INITIAL + 1, true));

    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL + 2, ChunkNumber::INITIAL, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL + 2, ChunkNumber::INITIAL + 1, false));
    inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL + 2, ChunkNumber::INITIAL + 2, true));

    bool hasMorePermutations = true;
    while (hasMorePermutations)
    {
        reset();
        EmitPhysicalOperator emit = createUUT();
        for (auto& buffer : inputBuffers)
        {
            run(
                [&](auto& executionContext, auto& recordBuffer)
                {
                    emit.open(executionContext, recordBuffer);
                    emit.close(executionContext, recordBuffer);
                },
                buffer);
        }
        checkNumberOfBuffers(8);
        checkForDups();
        checkLastChunks();
        hasMorePermutations = std::ranges::next_permutation(
                                  inputBuffers,
                                  std::less{},
                                  [](const TupleBuffer& buffer)
                                  { return SequenceData(buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk()); })
                                  .found;
    };
}

TEST_F(EmitPhysicalOperatorTest, ConcurrentSequenceChunkNumberTest)
{
    for (auto [numberOfSequences, maxChunksPerSequence, numberOfThreads] :
         std::initializer_list<std::tuple<size_t, size_t, size_t>>{{2, 10, 2}, {1000, 2, 4}, {10, 100, 4}, {1000, 20, 10}})
    {
        reset();
        std::vector<TupleBuffer> inputBuffers;
        for (size_t seq = 0; seq < numberOfSequences; seq++)
        {
            std::uniform_int_distribution chunkNumbers(ChunkNumber::INITIAL + 1, maxChunksPerSequence);
            auto maxChunkForThisSequence = chunkNumbers(rd);
            for (size_t chunk = 0; chunk < maxChunkForThisSequence - 1; chunk++)
            {
                inputBuffers.emplace_back(createBuffer(SequenceNumber::INITIAL + seq, ChunkNumber::INITIAL + chunk, false));
            }
            inputBuffers.emplace_back(
                createBuffer(SequenceNumber::INITIAL + seq, ChunkNumber::INITIAL + maxChunkForThisSequence - 1, true));
        }

        EmitPhysicalOperator emit = createUUT();
        std::ranges::shuffle(inputBuffers, rd);
        std::barrier<> barrier(static_cast<int>(numberOfThreads) + 1);
        std::vector<std::jthread> threads;
        threads.reserve(numberOfThreads);
        for (size_t threadId = 0; threadId < numberOfThreads; threadId++)
        {
            threads.emplace_back(
                [threadId, &inputBuffers, this, &emit, &barrier, numberOfThreads]()
                {
                    barrier.arrive_and_wait();
                    for (size_t index = threadId; index < inputBuffers.size(); index += numberOfThreads)
                    {
                        run(
                            [&](auto& executionContext, auto& recordBuffer)
                            {
                                emit.open(executionContext, recordBuffer);
                                emit.close(executionContext, recordBuffer);
                            },
                            inputBuffers.at(index));
                    }
                });
        }
        barrier.arrive_and_wait();
        threads.clear();

        checkNumberOfBuffers(inputBuffers.size());
        checkForDups();
        checkLastChunks();
    }
}
}
