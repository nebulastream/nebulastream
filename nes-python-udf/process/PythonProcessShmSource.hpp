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

#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <span>
#include <ostream>
#include <stop_token>
#include <vector>

#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/SharedMemoryAllocator.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <nanoarrow/nanoarrow.hpp>
#include <ArrowBatches.hpp>
#include <PythonBatchWrapper.hpp>
#include <PythonProcessChannel.hpp>
#include <PythonProcesses.hpp>
#include <UdfProcessCall.hpp>

namespace NES
{

struct PythonProcessSourceConfig;

/// The PythonProcess source for SHARED_MEMORY, see PythonProcessSource. Each process shares a memory file with the worker that holds a
/// ring of requests and a ring of answers, see UdfProcessProtocol::SHARED_MEMORY. The source thread alone drives all processes: it
/// copies each batch into the request ring of a process, and copies the answers out of the answer rings into output buffers, without
/// system calls while there is work. Only when neither the processes nor the channel have anything for it does it spin for a while
/// and then sleep, until a process or the channel wakes it. With SHARED_MEMORY_ZERO_COPY, nothing is copied: the processes read the
/// batches in the pool and write the results into their empty result columns, and the source passes the batches on as they are.
class PythonProcessShmSource final : public Source
{
public:
    /// `protocol` is SHARED_MEMORY or SHARED_MEMORY_ZERO_COPY.
    PythonProcessShmSource(const PythonProcessSourceConfig& config, const SourceDescriptor& sourceDescriptor, UdfProcessProtocol protocol);
    ~PythonProcessShmSource() override;
    PythonProcessShmSource(const PythonProcessShmSource&) = delete;
    PythonProcessShmSource& operator=(const PythonProcessShmSource&) = delete;
    PythonProcessShmSource(PythonProcessShmSource&&) = delete;
    PythonProcessShmSource& operator=(PythonProcessShmSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    /// Without copies, the batches carry their answers themselves and go on as they are, so the source needs no buffers to fill.
    [[nodiscard]] bool providesBuffers() const override { return zeroCopy; }
    std::optional<TupleBuffer> nextBuffer(const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    /// Origin id, sequence and chunk number and watermark are taken over from the input.
    [[nodiscard]] bool addsMetadata() const override { return true; }

    /// Public, so that PythonProcessSource can print the implementation it chose.
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// A batch a process has been sent and not answered yet.
    struct InFlight
    {
        TupleBuffer input;
    };

    /// A process with its shared memory and the batches it has been sent and not answered yet, oldest first.
    struct Process
    {
        PythonProcesses::Process handle;
        std::byte* memory = nullptr;
        std::deque<InFlight> inFlight;
        /// SHARED_MEMORY_ZERO_COPY with VARSIZED results: the buffers of the pool the process was given for their values, the one it
        /// writes into first. Each answer whose values are in one becomes its owner too, so it lives on after the process moved on.
        std::deque<TupleBuffer> valueAreas;
        /// The bytes of the request ring this source published, and of the answer ring it took.
        uint64_t requestTail = 0;
        uint64_t answerHead = 0;
    };

    /// A batch with its answer, or without one if it had no rows.
    struct Result
    {
        TupleBuffer input;
        std::optional<TupleBuffer> output;
    };

    /// Takes batches from the channel for processes with room for them; returns whether it sent any. Sets `channelEnded`.
    bool feedProcesses();
    /// Makes `unsent` the next batch from the channel with rows, unless it already is one; false if the channel has none now.
    bool takeInput();
    /// Puts `input` into the request ring of `process` and wakes it if it sleeps; false if the ring has no room for it now. sendCopy
    /// copies the columns into the ring, sendInPlace only says where in the pool they are.
    [[nodiscard]] bool sendCopy(Process& process, const TupleBuffer& input);
    [[nodiscard]] bool sendInPlace(Process& process, const TupleBuffer& input);
    /// Where a request of `record` bytes goes in the ring of `process`, or nullptr if there is no room for it now.
    static std::byte* reserveRequest(Process& process, size_t record);
    static void publishRequest(Process& process, size_t record);
    /// Wakes `process` if it sleeps, after requests were published.
    static void wakeIfAsleep(Process& process);
    /// Turns the answer `record` to `flight` into `output`: copies its body.
    void receiveCopy(const InFlight& flight, std::span<const std::byte> record, TupleBuffer& output) const;
    /// Completes the input of `flight` with the answer `record`, whose results the process wrote into it: points its header at the
    /// VARSIZED values and drops the bitmaps of results without NULLs.
    void receiveInPlace(Process& process, InFlight& flight, std::span<const std::byte> record) const;
    /// Takes the answers that arrived from `process`; returns whether there were any. Stops early on a stop request.
    bool receiveAnswers(Process& process, const std::stop_token& stopToken);
    /// Sleeps until a process or the channel wakes the source, or for a short while.
    void sleep();
    /// The next batch with its answer; nullopt at the end of the stream or on a stop request.
    std::optional<Result> nextResult(const std::stop_token& stopToken);

    PythonProcessChannel::Lease channel;
    bool zeroCopy;
    /// SHARED_MEMORY_ZERO_COPY: the pool of the worker, which the processes map.
    std::optional<SharedMemoryAllocator::Mapping> pool;
    std::vector<UdfProcessCall> calls;
    /// Whether a call returns VARSIZED values, which need value areas.
    bool varSizedResults = false;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::shared_ptr<ArrowBufferRef> layout;
    nanoarrow::UniqueSchema arrowSchema;
    std::vector<std::unique_ptr<Process>> processes;

    std::deque<Result> results;
    /// The process whose answers are taken next.
    size_t nextProcess = 0;
    /// A batch from the channel for which no ring had room yet.
    std::optional<TupleBuffer> unsent;
    /// A buffer the source loop handed `fillTupleBuffer` and that no answer took yet, used for the next one.
    std::optional<TupleBuffer> spare;
    bool channelEnded = false;

};

}
