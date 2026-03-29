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
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <numeric>
#include <random>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <cpptrace/from_current.hpp>
#include <gtest/gtest.h>
#include <BackpressureChannel.hpp>
#include <PipeService.hpp>
#include <PipeSink.hpp>
#include <PipeSource.hpp>
#include <PipelineExecutionContext.hpp>

using namespace NES;

/// Minimal stub — PipeSink ignores PipelineExecutionContext entirely.
class StubPipelineExecutionContext final : public PipelineExecutionContext
{
public:
    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return true; }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { }

    TupleBuffer allocateTupleBuffer() override { return {}; }

    TupleBuffer& pinBuffer(TupleBuffer&& buf) override
    {
        pinnedBuffers.push_back(std::move(buf));
        return pinnedBuffers.back();
    }

    [[nodiscard]] WorkerThreadId getId() const override { return WorkerThreadId(0); }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return nullptr; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return handlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>&) override { }

private:
    std::unordered_map<NES::OperatorHandlerId, std::shared_ptr<NES::OperatorHandler>> handlers;
    std::vector<TupleBuffer> pinnedBuffers;
};

class PipeIntegrationTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        schema = Schema{}.addField("id", DataType::Type::UINT64).addField("value", DataType::Type::UINT64);
        bufferManager = BufferManager::create(1024, 4096);
    }

    void TearDown() override { PipeService::instance().unregisterSink("test_pipe"); }

    std::unique_ptr<PipeSink> makePipeSink(BackpressureController bpController, const std::string& pipeName = "test_pipe")
    {
        auto desc = sinkCatalog.getInlineSink(schema, "Pipe", {{"pipe_name", pipeName}}, {});
        EXPECT_TRUE(desc.has_value());
        return std::make_unique<PipeSink>(std::move(bpController), desc.value());
    }

    std::unique_ptr<PipeSource> makePipeSource(const std::string& pipeName = "test_pipe")
    {
        auto desc = sourceCatalog.getInlineSource("Pipe", schema, {{"type", "NATIVE"}}, {{"pipe_name", pipeName}});
        EXPECT_TRUE(desc.has_value());
        return std::make_unique<PipeSource>(desc.value());
    }

    Schema schema;
    std::shared_ptr<BufferManager> bufferManager;
    SinkCatalog sinkCatalog;
    SourceCatalog sourceCatalog;
    StubPipelineExecutionContext pipeCtx;
};

TEST_F(PipeIntegrationTest, DataFlowThroughRealSinkAndSource)
{
    auto [bpController, bpListener] = createBackpressureChannel();
    auto sink = makePipeSink(std::move(bpController));
    sink->start(pipeCtx);

    auto source = makePipeSource();
    source->open(nullptr);

    /// Write data through the sink
    auto buffer = bufferManager->getBufferBlocking();
    auto mem = buffer.getAvailableMemoryArea<uint64_t>();
    mem[0] = 42;
    mem[1] = 99;
    buffer.setNumberOfTuples(1);
    buffer.setSequenceNumber(INITIAL_SEQ_NUMBER);
    buffer.setLastChunk(true);
    sink->execute(buffer, pipeCtx);

    /// Read from the source
    auto outBuffer = bufferManager->getBufferBlocking();
    const std::stop_source stopSource;
    auto result = source->fillTupleBuffer(outBuffer, stopSource.get_token());

    ASSERT_FALSE(result.isEoS());
    EXPECT_EQ(result.getNumberOfBytes(), 1U);

    auto outMem = outBuffer.getAvailableMemoryArea<uint64_t>();
    EXPECT_EQ(outMem[0], 42U);
    EXPECT_EQ(outMem[1], 99U);

    source->close();
    sink->stop(pipeCtx);
}

TEST_F(PipeIntegrationTest, EoSPropagation)
{
    auto [bpController, bpListener] = createBackpressureChannel();
    auto sink = makePipeSink(std::move(bpController));
    sink->start(pipeCtx);

    auto source = makePipeSource();
    source->open(nullptr);

    /// Send a data buffer to activate the pending source
    auto buffer = bufferManager->getBufferBlocking();
    buffer.setNumberOfTuples(0);
    buffer.setSequenceNumber(INITIAL_SEQ_NUMBER);
    buffer.setLastChunk(true);
    sink->execute(buffer, pipeCtx);

    /// Stop the sink — should send EoS to all sources
    sink->stop(pipeCtx);

    const std::stop_source stopSource;

    /// First read drains the activation buffer
    auto dataBuf = bufferManager->getBufferBlocking();
    auto dataResult = source->fillTupleBuffer(dataBuf, stopSource.get_token());
    ASSERT_FALSE(dataResult.isEoS());

    /// Second read should receive EoS
    auto eosBuf = bufferManager->getBufferBlocking();
    auto eosResult = source->fillTupleBuffer(eosBuf, stopSource.get_token());
    EXPECT_TRUE(eosResult.isEoS());

    source->close();
}

TEST_F(PipeIntegrationTest, DestructorPropagatesError)
{
    std::unique_ptr<PipeSource> source;
    {
        auto [bpController, bpListener] = createBackpressureChannel();
        auto sink = makePipeSink(std::move(bpController));
        sink->start(pipeCtx);

        source = makePipeSource();
        source->open(nullptr);

        /// Activate the source by sending a buffer
        auto buffer = bufferManager->getBufferBlocking();
        buffer.setNumberOfTuples(0);
        buffer.setSequenceNumber(INITIAL_SEQ_NUMBER);
        buffer.setLastChunk(true);
        sink->execute(buffer, pipeCtx);

        /// Sink destroyed without calling stop() — destructor sends PipeError
    }

    const std::stop_source stopSource;

    /// First read drains the activation buffer
    auto dataBuf = bufferManager->getBufferBlocking();
    auto dataResult = source->fillTupleBuffer(dataBuf, stopSource.get_token());
    ASSERT_FALSE(dataResult.isEoS());

    /// Second read should receive PipeError → returns EoS
    auto errBuf = bufferManager->getBufferBlocking();
    auto errResult = source->fillTupleBuffer(errBuf, stopSource.get_token());
    EXPECT_TRUE(errResult.isEoS());

    source->close();
}

TEST_F(PipeIntegrationTest, SourceStopsAtSequenceBoundary)
{
    auto [bpController, bpListener] = createBackpressureChannel();
    auto sink = makePipeSink(std::move(bpController));
    sink->start(pipeCtx);

    auto source = makePipeSource();
    source->open(nullptr);

    /// Send chunk 0 (activates pending source at sequence boundary)
    auto buf1 = bufferManager->getBufferBlocking();
    buf1.getAvailableMemoryArea<uint64_t>()[0] = 1;
    buf1.getAvailableMemoryArea<uint64_t>()[1] = 0;
    buf1.setNumberOfTuples(1);
    buf1.setSequenceNumber(INITIAL_SEQ_NUMBER);
    buf1.setChunkNumber(ChunkNumber(0));
    buf1.setLastChunk(false);
    sink->execute(buf1, pipeCtx);

    /// Send chunk 1 (last chunk of sequence)
    auto buf2 = bufferManager->getBufferBlocking();
    buf2.getAvailableMemoryArea<uint64_t>()[0] = 2;
    buf2.getAvailableMemoryArea<uint64_t>()[1] = 0;
    buf2.setNumberOfTuples(1);
    buf2.setSequenceNumber(INITIAL_SEQ_NUMBER);
    buf2.setChunkNumber(ChunkNumber(1));
    buf2.setLastChunk(true);
    sink->execute(buf2, pipeCtx);

    /// Request stop before reading — source should still deliver both chunks
    std::stop_source stopSource;
    stopSource.request_stop();

    auto out1 = bufferManager->getBufferBlocking();
    auto res1 = source->fillTupleBuffer(out1, stopSource.get_token());
    ASSERT_FALSE(res1.isEoS());
    EXPECT_EQ(out1.getAvailableMemoryArea<uint64_t>()[0], 1U);

    auto out2 = bufferManager->getBufferBlocking();
    auto res2 = source->fillTupleBuffer(out2, stopSource.get_token());
    ASSERT_FALSE(res2.isEoS());
    EXPECT_EQ(out2.getAvailableMemoryArea<uint64_t>()[0], 2U);

    /// Now at sequence boundary (lastChunk was true) — next read should return eos
    auto out3 = bufferManager->getBufferBlocking();
    auto res3 = source->fillTupleBuffer(out3, stopSource.get_token());
    EXPECT_TRUE(res3.isEoS());

    source->close();
    sink->stop(pipeCtx);
}

TEST_F(PipeIntegrationTest, StaggeredConsumersChunkedDataIntegrity)
{
    static constexpr uint64_t numTuples = 100000;
    static constexpr uint64_t tuplesPerChunk = 1;
    static constexpr uint64_t chunksPerSeq = 8;
    static constexpr uint64_t tuplesPerSequence = tuplesPerChunk * chunksPerSeq;
    static constexpr int numLateJoiners = 4;

    auto [bpController, bpListener] = createBackpressureChannel();
    auto sink = makePipeSink(std::move(bpController));
    sink->start(pipeCtx);

    /// Build values with intra-sequence OOO shuffling
    std::vector<uint64_t> values(numTuples);
    for (uint64_t idx = 0; idx < numTuples; ++idx)
    {
        values[idx] = idx;
    }
    {
        std::mt19937 rng(42);
        for (uint64_t i = 0; i + tuplesPerSequence <= numTuples; i += tuplesPerSequence)
        {
            std::shuffle(
                values.begin() + static_cast<std::ptrdiff_t>(i), values.begin() + static_cast<std::ptrdiff_t>(i + tuplesPerSequence), rng);
        }
    }

    /// Consumer helper: reads from an already-opened PipeSource until EoS
    auto consumeFrom = [this](PipeSource& src) -> std::vector<uint64_t>
    {
        std::vector<uint64_t> received;
        std::stop_source stopSrc;
        while (true)
        {
            auto buf = bufferManager->getBufferBlocking();
            auto result = src.fillTupleBuffer(buf, stopSrc.get_token());
            if (result.isEoS())
            {
                break;
            }
            auto mem = buf.getAvailableMemoryArea<uint64_t>();
            for (uint64_t idx = 0; idx < result.getNumberOfBytes(); ++idx)
            {
                received.push_back(mem[idx * 2]);
            }
        }
        return received;
    };

    /// Open consumer 1 before producer starts
    auto src1 = makePipeSource();
    src1->open(nullptr);
    std::vector<uint64_t> result1;
    auto consumer1Thread = std::jthread([&] { result1 = consumeFrom(*src1); });

    /// Producer: blasts multi-chunk sequences (2 chunks of 4 tuples each), no pauses
    std::atomic<bool> producerRunning{false};
    auto producer = std::jthread(
        [&]
        {
            producerRunning.store(true);
            uint64_t produced = 0;
            size_t seqNumCounter = SequenceNumber::INITIAL;
            while (produced < numTuples)
            {
                for (uint64_t chunk = 0; chunk < chunksPerSeq && produced < numTuples; ++chunk)
                {
                    auto buffer = bufferManager->getBufferBlocking();
                    auto mem = buffer.getAvailableMemoryArea<uint64_t>();
                    uint64_t count = std::min(tuplesPerChunk, numTuples - produced);
                    for (uint64_t idx = 0; idx < count; ++idx)
                    {
                        mem[idx * 2] = values[produced + idx];
                        mem[(idx * 2) + 1] = 0;
                    }
                    buffer.setNumberOfTuples(count);
                    buffer.setSequenceNumber(SequenceNumber(seqNumCounter));
                    buffer.setChunkNumber(ChunkNumber(chunk));
                    buffer.setLastChunk(chunk == chunksPerSeq - 1 || produced + count >= numTuples);
                    sink->execute(buffer, pipeCtx);
                    produced += count;
                }
                ++seqNumCounter;
            }
            sink->stop(pipeCtx);
        });

    /// Spawn multiple late-joining consumer threads concurrently while the producer runs.
    /// Each thread opens a PipeSource, reads until EoS, and closes.
    /// With 8 chunks per sequence, there are 7 inter-chunk gaps per sequence where
    /// a join can race the producer — maximizing the chance of triggering the bug.
    while (!producerRunning.load())
    {
        std::this_thread::yield();
    }

    std::vector<std::vector<uint64_t>> lateResults(numLateJoiners);
    std::vector<std::thread> lateThreads;
    lateThreads.reserve(numLateJoiners);

    for (int i = 0; i < numLateJoiners; ++i)
    {
        lateThreads.emplace_back(
            [&, i]
            {
                auto src = makePipeSource();
                CPPTRACE_TRY
                {
                    src->open(nullptr);
                }
                CPPTRACE_CATCH(...)
                {
                    return;
                }
                lateResults[i] = consumeFrom(*src);
                src->close();
            });
    }

    producer.join();
    consumer1Thread.join();
    for (auto& thr : lateThreads)
    {
        thr.join();
    }

    src1->close();

    /// Verify no gaps via Gauss sum
    auto verifyNoGaps = [](std::vector<uint64_t>& data, const std::string& label)
    {
        ASSERT_FALSE(data.empty()) << label << ": received no data";
        std::ranges::sort(data);
        uint64_t minVal = data.front();
        uint64_t maxVal = data.back();
        ASSERT_EQ(data.size(), maxVal - minVal + 1) << label << ": expected contiguous range";
        uint64_t expectedSum = maxVal * (maxVal + 1) / 2;
        if (minVal > 0)
        {
            expectedSum -= (minVal - 1) * minVal / 2;
        }
        uint64_t actualSum = std::accumulate(data.begin(), data.end(), uint64_t{0});
        EXPECT_EQ(actualSum, expectedSum) << label << ": Gauss sum mismatch";
    };

    EXPECT_EQ(result1.size(), numTuples) << "Consumer 1 should receive all data";
    verifyNoGaps(result1, "Consumer1");

    for (int i = 0; i < numLateJoiners; ++i)
    {
        if (!lateResults[i].empty())
        {
            verifyNoGaps(lateResults[i], "LateConsumer" + std::to_string(i));
        }
    }
}
