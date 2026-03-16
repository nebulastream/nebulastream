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

#include <Inference/IREEInferenceOperatorHandler.hpp>
#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <InferModelPhysicalOperator.hpp>
#include <ModelLoader.hpp>
#include <PhysicalOperator.hpp>
#include <ScanPhysicalOperator.hpp>

#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <folly/Synchronized.h>
#include <gtest/gtest.h>
#include <nautilus/Engine.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <Pipeline.hpp>
#include <PipelineExecutionContext.hpp>
#include <TestTupleBuffer.hpp>

namespace NES
{

constexpr size_t bufferSize = 8192;

class InferModelPhysicalOperatorTest : public Testing::BaseUnitTest
{
protected:
    struct MockedPipelineContext final : PipelineExecutionContext
    {
        bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
        {
            buffers.wlock()->emplace_back(buffer);
            return true;
        }

        TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

        [[nodiscard]] WorkerThreadId getId() const override { return threadId; }

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

        TupleBuffer& pinBuffer(TupleBuffer&& tupleBuffer) override
        {
            pinnedBuffers.emplace_back(std::make_unique<TupleBuffer>(tupleBuffer));
            return *pinnedBuffers.back();
        }

        ///NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) lifetime is ensured by the fixture
        folly::Synchronized<std::vector<TupleBuffer>>& buffers;
        std::shared_ptr<BufferManager> bufferManager;
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* operatorHandlers = nullptr;
        std::vector<std::unique_ptr<TupleBuffer>> pinnedBuffers;
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

    /// Result of createInferencePipeline — everything needed to run the pipeline.
    struct InferencePipeline
    {
        std::shared_ptr<Pipeline> pipeline;
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
    };

public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("InferModelPhysicalOperatorTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup InferModelPhysicalOperatorTest test class.");

        ASSERT_TRUE(NES::Inference::enabled()) << "IREE inference tools not available — test binary should not be built without them";

        const std::string base = std::string(INFERENCE_TEST_DATA) + "/";

        auto identityResult = NES::Inference::load(base + "tiny_identity.onnx", {});
        ASSERT_TRUE(identityResult.has_value()) << "Failed to load identity model — likely an infrastructure problem with IREE tools";
        identityModel = std::move(*identityResult);

        auto reductionResult = NES::Inference::load(base + "tiny_reduction.onnx", {});
        ASSERT_TRUE(reductionResult.has_value()) << "Failed to load reduction model — likely an infrastructure problem with IREE tools";
        reductionModel = std::move(*reductionResult);

        auto expansionResult = NES::Inference::load(base + "tiny_expansion.onnx", {});
        ASSERT_TRUE(expansionResult.has_value()) << "Failed to load expansion model — likely an infrastructure problem with IREE tools";
        expansionModel = std::move(*expansionResult);
    }

    void SetUp() override { BaseUnitTest::SetUp(); }

    static std::string packFloatsToVarsized(const std::vector<float>& floats)
    {
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) float-to-byte packing
        const char* rawData = reinterpret_cast<const char*>(floats.data());
        return std::string(rawData, floats.size() * sizeof(float));
    }

    static std::vector<float> makeFloats(size_t count, float startValue = 1.0F)
    {
        std::vector<float> vec(count);
        /// NOLINTNEXTLINE(modernize-use-ranges) std::ranges::iota not yet available in libc++
        std::iota(vec.begin(), vec.end(), startValue);
        return vec;
    }

    static std::vector<std::string> makeOutputFieldNames(size_t count)
    {
        std::vector<std::string> names;
        names.reserve(count);
        for (size_t i = 0; i < count; ++i)
        {
            names.push_back("out_" + std::to_string(i));
        }
        return names;
    }

    /// Builds the input/output schemas for a VARSIZED-input inference test.
    static std::pair<Schema, Schema> makeSchemas(const std::vector<std::string>& outputFieldNames)
    {
        Schema inputSchema;
        inputSchema.addField("input_blob", DataType::Type::VARSIZED);

        Schema outputSchema;
        outputSchema.addField("input_blob", DataType::Type::VARSIZED);
        for (const auto& name : outputFieldNames)
        {
            outputSchema.addField(name, DataType::Type::FLOAT32);
        }
        return {inputSchema, outputSchema};
    }

    /// Creates a Scan -> InferModel -> Emit pipeline with the given model and schemas.
    static InferencePipeline createInferencePipeline(
        const NES::Model& model,
        const Schema& inputSchema,
        const Schema& outputSchema,
        const std::vector<std::string>& inputFieldNames,
        const std::vector<std::string>& outputFieldNames,
        bool varsizedInput,
        bool varsizedOutput)
    {
        auto inputBufRef = LowerSchemaProvider::lowerSchema(bufferSize, inputSchema, MemoryLayoutType::ROW_LAYOUT);
        auto outputBufRef = LowerSchemaProvider::lowerSchema(bufferSize, outputSchema, MemoryLayoutType::ROW_LAYOUT);

        ScanPhysicalOperator scan(inputBufRef, inputSchema.getFieldNames());
        InferModelPhysicalOperator inferModel(OperatorHandlerId(0), inputFieldNames, outputFieldNames, varsizedInput, varsizedOutput);
        EmitPhysicalOperator emit(OperatorHandlerId(1), outputBufRef);

        inferModel.setChild(PhysicalOperator(emit));
        scan.setChild(PhysicalOperator(inferModel));

        auto pipeline = std::make_shared<Pipeline>(PhysicalOperator(scan));

        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
        handlers[OperatorHandlerId(0)] = std::make_shared<NES::Inference::IREEInferenceOperatorHandler>(model);
        handlers[OperatorHandlerId(1)] = std::make_shared<EmitOperatorHandler>();

        return {.pipeline = std::move(pipeline), .handlers = std::move(handlers)};
    }

    /// Creates an input TupleBuffer with VARSIZED records, each containing packed floats.
    TupleBuffer createInputBuffer(const Schema& inputSchema, const std::vector<std::vector<float>>& recordFloats) const
    {
        auto bufMgr = BufferManager::create(bufferSize, 200);
        auto tupleBuffer = bufMgr->getBufferBlocking();
        tupleBuffer.setSequenceNumber(SequenceNumber(1));
        tupleBuffer.setChunkNumber(ChunkNumber(1));
        tupleBuffer.setLastChunk(true);
        tupleBuffer.setOriginId(INITIAL<OriginId>);

        Testing::TestTupleBuffer ttb(inputSchema);
        auto view = ttb.open(tupleBuffer, bufMgr.get());
        for (const auto& floats : recordFloats)
        {
            view.append(packFloatsToVarsized(floats));
        }
        return tupleBuffer;
    }

    static inline std::optional<NES::Model> identityModel;
    static inline std::optional<NES::Model> reductionModel;
    static inline std::optional<NES::Model> expansionModel;
};

/// Identity model with 1 record of 100 floats. Output matches input. Interpreted + compiled.
TEST_F(InferModelPhysicalOperatorTest, IdentityModelCorrectness)
{
    constexpr size_t numFloats = 100;
    const auto outputFieldNames = makeOutputFieldNames(numFloats);
    const auto [inputSchema, outputSchema] = makeSchemas(outputFieldNames);
    auto inputBuffer = createInputBuffer(inputSchema, {makeFloats(numFloats)});

    for (bool compiled : {false, true})
    {
        auto [pipeline, handlers]
            = createInferencePipeline(*identityModel, inputSchema, outputSchema, {"input_blob"}, outputFieldNames, true, false);
        CompiledExecutablePipelineStage stage(
            pipeline,
            handlers,
            [&]
            {
                nautilus::engine::Options opt;
                opt.setOption("engine.Compilation", compiled);
                return opt;
            }());

        folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
        emittedBuffers.wlock()->reserve(16);
        auto bufMgr = BufferManager::create(bufferSize, 200);
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
            for (size_t r = 0; r < view.getNumberOfTuples(); ++r)
            {
                for (size_t i = 0; i < numFloats; ++i)
                {
                    EXPECT_NEAR(view[r]["out_" + std::to_string(i)].as<float>(), static_cast<float>(i + 1), 1e-5F)
                        << "Field out_" << i << " mismatch (compiled=" << compiled << ")";
                }
                ++totalRecords;
            }
        }
        EXPECT_EQ(totalRecords, 1U) << "(compiled=" << compiled << ")";
    }
}

/// Reduction model (100->10) with 1 record. Outputs are zeros. Interpreted + compiled.
TEST_F(InferModelPhysicalOperatorTest, ReductionModelCorrectness)
{
    constexpr size_t numOutputFloats = 10;
    const auto outputFieldNames = makeOutputFieldNames(numOutputFloats);
    const auto [inputSchema, outputSchema] = makeSchemas(outputFieldNames);
    auto inputBuffer = createInputBuffer(inputSchema, {makeFloats(100)});

    for (bool compiled : {false, true})
    {
        auto [pipeline, handlers]
            = createInferencePipeline(*reductionModel, inputSchema, outputSchema, {"input_blob"}, outputFieldNames, true, false);
        CompiledExecutablePipelineStage stage(
            pipeline,
            handlers,
            [&]
            {
                nautilus::engine::Options opt;
                opt.setOption("engine.Compilation", compiled);
                return opt;
            }());

        folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
        emittedBuffers.wlock()->reserve(16);
        auto bufMgr = BufferManager::create(bufferSize, 200);
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
            for (size_t r = 0; r < view.getNumberOfTuples(); ++r)
            {
                for (size_t i = 0; i < numOutputFloats; ++i)
                {
                    EXPECT_NEAR(view[r]["out_" + std::to_string(i)].as<float>(), 0.0F, 1e-5F)
                        << "Field out_" << i << " expected zero (compiled=" << compiled << ")";
                }
                ++totalRecords;
            }
        }
        EXPECT_EQ(totalRecords, 1U) << "(compiled=" << compiled << ")";
    }
}

/// Expansion model (10->100) with 1 record. Outputs are zeros. Interpreted + compiled.
TEST_F(InferModelPhysicalOperatorTest, ExpansionModelCorrectness)
{
    constexpr size_t numOutputFloats = 100;
    const auto outputFieldNames = makeOutputFieldNames(numOutputFloats);
    const auto [inputSchema, outputSchema] = makeSchemas(outputFieldNames);
    auto inputBuffer = createInputBuffer(inputSchema, {makeFloats(10)});

    for (bool compiled : {false, true})
    {
        auto [pipeline, handlers]
            = createInferencePipeline(*expansionModel, inputSchema, outputSchema, {"input_blob"}, outputFieldNames, true, false);
        CompiledExecutablePipelineStage stage(
            pipeline,
            handlers,
            [&]
            {
                nautilus::engine::Options opt;
                opt.setOption("engine.Compilation", compiled);
                return opt;
            }());

        folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
        emittedBuffers.wlock()->reserve(16);
        auto bufMgr = BufferManager::create(bufferSize, 200);
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
            for (size_t r = 0; r < view.getNumberOfTuples(); ++r)
            {
                for (size_t i = 0; i < numOutputFloats; ++i)
                {
                    EXPECT_NEAR(view[r]["out_" + std::to_string(i)].as<float>(), 0.0F, 1e-5F)
                        << "Field out_" << i << " expected zero (compiled=" << compiled << ")";
                }
                ++totalRecords;
            }
        }
        EXPECT_EQ(totalRecords, 1U) << "(compiled=" << compiled << ")";
    }
}

/// Identity model with 5 records per buffer. Per-record correctness. Interpreted + compiled.
TEST_F(InferModelPhysicalOperatorTest, MultiRecordIdentity)
{
    constexpr size_t numFloats = 100;
    constexpr size_t numRecords = 5;
    const auto outputFieldNames = makeOutputFieldNames(numFloats);
    const auto [inputSchema, outputSchema] = makeSchemas(outputFieldNames);

    std::vector<std::vector<float>> inputs;
    inputs.reserve(numRecords);
    for (size_t r = 0; r < numRecords; ++r)
    {
        inputs.push_back(makeFloats(numFloats, static_cast<float>((r * numFloats) + 1)));
    }
    auto inputBuffer = createInputBuffer(inputSchema, inputs);

    for (bool compiled : {false, true})
    {
        auto [pipeline, handlers]
            = createInferencePipeline(*identityModel, inputSchema, outputSchema, {"input_blob"}, outputFieldNames, true, false);
        CompiledExecutablePipelineStage stage(
            pipeline,
            handlers,
            [&]
            {
                nautilus::engine::Options opt;
                opt.setOption("engine.Compilation", compiled);
                return opt;
            }());

        folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
        emittedBuffers.wlock()->reserve(16);
        auto bufMgr = BufferManager::create(bufferSize, 200);
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
            for (size_t r = 0; r < view.getNumberOfTuples(); ++r)
            {
                for (size_t i = 0; i < numFloats; ++i)
                {
                    const auto expected = static_cast<float>((totalRecords * numFloats) + i + 1);
                    EXPECT_NEAR(view[r]["out_" + std::to_string(i)].as<float>(), expected, 1e-5F)
                        << "Record " << totalRecords << " field out_" << i << " (compiled=" << compiled << ")";
                }
                ++totalRecords;
            }
        }
        EXPECT_EQ(totalRecords, numRecords) << "(compiled=" << compiled << ")";
    }
}

/// Zero-record buffer produces zero output records. Interpreted + compiled.
TEST_F(InferModelPhysicalOperatorTest, ZeroRecordBuffer)
{
    constexpr size_t numFloats = 100;
    const auto outputFieldNames = makeOutputFieldNames(numFloats);
    const auto [inputSchema, outputSchema] = makeSchemas(outputFieldNames);
    auto inputBuffer = createInputBuffer(inputSchema, {});

    for (bool compiled : {false, true})
    {
        auto [pipeline, handlers]
            = createInferencePipeline(*identityModel, inputSchema, outputSchema, {"input_blob"}, outputFieldNames, true, false);
        CompiledExecutablePipelineStage stage(
            pipeline,
            handlers,
            [&]
            {
                nautilus::engine::Options opt;
                opt.setOption("engine.Compilation", compiled);
                return opt;
            }());

        folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
        emittedBuffers.wlock()->reserve(16);
        auto bufMgr = BufferManager::create(bufferSize, 200);
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

/// 8 threads, 200 buffers each, shared compiled pipeline. Verifies correctness under concurrency.
TEST_F(InferModelPhysicalOperatorTest, ConcurrentStressTest)
{
    constexpr size_t numFloats = 100;
    constexpr size_t numThreads = 8;
    constexpr size_t buffersPerThread = 200;
    const auto outputFieldNames = makeOutputFieldNames(numFloats);
    const auto [inputSchema, outputSchema] = makeSchemas(outputFieldNames);

    /// Single shared pipeline
    auto [pipeline, handlers]
        = createInferencePipeline(*identityModel, inputSchema, outputSchema, {"input_blob"}, outputFieldNames, true, false);

    nautilus::engine::Options options;
    options.setOption("engine.Compilation", true);
    CompiledExecutablePipelineStage stage(pipeline, handlers, options);

    folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
    emittedBuffers.wlock()->reserve(numThreads * buffersPerThread * 2);
    auto bufMgr = BufferManager::create(bufferSize, numThreads * buffersPerThread * 4);

    {
        MockedPipelineContext startPec{emittedBuffers, bufMgr, INITIAL<WorkerThreadId>, static_cast<uint64_t>(numThreads)};
        stage.start(startPec);
    }

    const auto inputFloats = makeFloats(numFloats);

    /// Pre-create input buffers with unique sequence numbers
    std::vector<std::vector<TupleBuffer>> threadInputBuffers(numThreads);
    for (size_t t = 0; t < numThreads; ++t)
    {
        threadInputBuffers[t].reserve(buffersPerThread);
        for (size_t b = 0; b < buffersPerThread; ++b)
        {
            auto buf = createInputBuffer(inputSchema, {inputFloats});
            buf.setSequenceNumber(SequenceNumber((t * buffersPerThread) + b + 1));
            buf.setChunkNumber(INITIAL_CHUNK_NUMBER);
            buf.setLastChunk(true);
            buf.setOriginId(INITIAL<OriginId>);
            threadInputBuffers[t].push_back(std::move(buf));
        }
    }

    std::barrier<> startBarrier(static_cast<int>(numThreads) + 1);
    std::vector<std::jthread> threads;
    threads.reserve(numThreads);

    for (size_t t = 0; t < numThreads; ++t)
    {
        threads.emplace_back(
            [t, &threadInputBuffers, &stage, &emittedBuffers, &bufMgr, &startBarrier]()
            {
                MockedPipelineContext threadPec{emittedBuffers, bufMgr, WorkerThreadId(t), static_cast<uint64_t>(numThreads)};
                startBarrier.arrive_and_wait();
                for (auto& buf : threadInputBuffers[t])
                {
                    stage.execute(buf, threadPec);
                }
            });
    }

    startBarrier.arrive_and_wait();
    threads.clear();

    {
        MockedPipelineContext stopPec{emittedBuffers, bufMgr, INITIAL<WorkerThreadId>, static_cast<uint64_t>(numThreads)};
        stage.stop(stopPec);
    }

    const size_t expectedTotalRecords = numThreads * buffersPerThread;
    size_t totalRecords = 0;
    size_t correctRecords = 0;

    auto outputBuffers = *emittedBuffers.rlock();
    for (auto& outBuf : outputBuffers)
    {
        Testing::TestTupleBuffer ttb(outputSchema);
        auto view = ttb.open(outBuf);
        for (size_t r = 0; r < view.getNumberOfTuples(); ++r)
        {
            bool recordCorrect = true;
            for (size_t i = 0; i < numFloats; ++i)
            {
                if (std::abs(view[r]["out_" + std::to_string(i)].as<float>() - static_cast<float>(i + 1)) > 1e-5F)
                {
                    recordCorrect = false;
                    break;
                }
            }
            if (recordCorrect)
            {
                ++correctRecords;
            }
            ++totalRecords;
        }
    }

    EXPECT_EQ(totalRecords, expectedTotalRecords) << "Expected " << expectedTotalRecords << " total output records";
    EXPECT_EQ(correctRecords, expectedTotalRecords) << "All output records should match identity model input";
}

/// Identity model with 1 record, output collected as a single VARSIZED blob. Interpreted + compiled.
TEST_F(InferModelPhysicalOperatorTest, VarsizedOutputCorrectness)
{
    constexpr size_t numFloats = 100;

    /// Input schema: VARSIZED blob of packed floats
    Schema inputSchema;
    inputSchema.addField("input_blob", DataType::Type::VARSIZED);

    /// Output schema: input blob + single VARSIZED output blob
    Schema outputSchema;
    outputSchema.addField("input_blob", DataType::Type::VARSIZED);
    outputSchema.addField("output_blob", DataType::Type::VARSIZED);

    auto inputBuffer = createInputBuffer(inputSchema, {makeFloats(numFloats)});
    ASSERT_TRUE(identityModel.has_value());

    for (bool compiled : {false, true})
    {
        auto [pipeline, handlers] = createInferencePipeline(
            *identityModel, inputSchema, outputSchema, {"input_blob"}, {"output_blob"}, /*varsizedInput=*/true, /*varsizedOutput=*/true);
        CompiledExecutablePipelineStage stage(
            pipeline,
            handlers,
            [&]
            {
                nautilus::engine::Options opt;
                opt.setOption("engine.Compilation", compiled);
                return opt;
            }());

        folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
        emittedBuffers.wlock()->reserve(16);
        auto bufMgr = BufferManager::create(bufferSize, 200);
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
            for (size_t r = 0; r < view.getNumberOfTuples(); ++r)
            {
                auto outputBlob = view[r]["output_blob"].as<std::string>();
                ASSERT_EQ(outputBlob.size(), numFloats * sizeof(float)) << "Output blob size mismatch (compiled=" << compiled << ")";

                /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) byte-to-float unpacking
                const auto* floatPtr = reinterpret_cast<const float*>(outputBlob.data());
                for (size_t i = 0; i < numFloats; ++i)
                {
                    EXPECT_NEAR(floatPtr[i], static_cast<float>(i + 1), 1e-5F)
                        << "Float " << i << " mismatch in output blob (compiled=" << compiled << ")";
                }
                ++totalRecords;
            }
        }
        EXPECT_EQ(totalRecords, 1U) << "(compiled=" << compiled << ")";
    }
}

}
