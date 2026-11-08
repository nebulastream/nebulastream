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

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include <sys/types.h>

#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <nanoarrow/nanoarrow.hpp>
#include <nanoarrow/nanoarrow_ipc.hpp>
#include <PythonBatchWrapper.hpp>
#include <PythonProcessChannel.hpp>
#include <UdfProcessCall.hpp>

namespace NES
{

struct PythonProcessSourceConfig;

/// The PythonProcess source for ARROW_IPC and RAW_BUFFERS, which exchange batches over the sockets of the processes, see
/// PythonProcessSource. The source thread alone drives all processes: it polls the channel and their sockets, which never block it, hands each process the next batch
/// while it still works on the one before, and reads the answers as they arrive, so no worker thread ever waits for Python.
/// Batches travel as Arrow IPC streams, which the source encodes with nanoarrow and the process reads and writes with pyarrow, or as the raw Arrow buffers of
/// the columns, which need no extension module in the process, so that it can be PyPy. Either way the process answers with the input
/// columns plus the results, which are read straight into an output buffer whose header then points at them. The source passes the
/// output buffers on with the metadata of their input, in the order the processes finish them.
class PythonProcessSocketSource final : public Source
{
public:
    PythonProcessSocketSource(const PythonProcessSourceConfig& config, const SourceDescriptor& sourceDescriptor, UdfProcessProtocol protocol);
    ~PythonProcessSocketSource() override;
    PythonProcessSocketSource(const PythonProcessSocketSource&) = delete;
    PythonProcessSocketSource& operator=(const PythonProcessSocketSource&) = delete;
    PythonProcessSocketSource(PythonProcessSocketSource&&) = delete;
    PythonProcessSocketSource& operator=(PythonProcessSocketSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    /// Origin id, sequence and chunk number and watermark are taken over from the input.
    [[nodiscard]] bool addsMetadata() const override { return true; }

    /// Public, so that PythonProcessSource can print the implementation it chose.
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// What the source reads from a process next.
    enum class Step : uint8_t
    {
        /// 1, or 0 and a failure follows.
        STATUS,
        /// The length of the failure message, and the message.
        FAILURE_LENGTH,
        FAILURE_MESSAGE,
        /// RAW_BUFFERS: [rows][body size].
        RAW_HEADER,
        /// ARROW_IPC: the continuation and metadata size of the next message, and the metadata.
        IPC_PREFIX,
        IPC_METADATA,
        /// The body of the answer, straight into the output buffer.
        BODY,
    };

    /// A Python process, with the batches it has been sent and what it still has to send and read of them.
    struct Process
    {
        pid_t pid = -1;
        int socket = -1;
        nanoarrow::ipc::UniqueEncoder encoder;
        nanoarrow::ipc::UniqueDecoder decoder;

        /// Sent or being sent, oldest first; the process answers them in this order.
        std::deque<TupleBuffer> inFlight;

        /// The rest of the batch being sent. The pieces point into the input buffer, which `inFlight` holds, or into what follows.
        std::vector<std::span<const std::byte>> sending;
        std::vector<uint64_t> sizes;
        nanoarrow::UniqueBuffer ipcHeader;
        nanoarrow::UniqueBuffer ipcBody;

        Step step = Step::STATUS;
        /// The bytes of the current step that have not arrived yet.
        std::span<std::byte> expected;
        /// What one call read from the socket and the steps have not taken yet, [inboxBegin, inboxEnd).
        std::array<std::byte, 64 * 1024> inbox{};
        size_t inboxBegin = 0;
        size_t inboxEnd = 0;
        std::array<uint64_t, 2> words{};
        std::vector<std::byte> message;
        std::optional<TupleBuffer> output;
        ArrowBufferRef::Location bodyLocation{};
        uint64_t bodySize = 0;
    };

    /// A batch with its answer, or without one if it had no rows.
    struct Result
    {
        TupleBuffer input;
        std::optional<TupleBuffer> output;
    };


    /// Takes batches from the channel for processes with room for one; false once the channel ended.
    bool feedProcesses();
    /// Queues the pieces of the batch for `process` to send, as UdfProcessProtocol says.
    void prepareRawBatch(Process& process, const TupleBuffer& input) const;
    void prepareIpcBatch(Process& process, const TupleBuffer& input) const;
    /// Sends what the socket takes without blocking.
    static void continueSending(Process& process);
    /// Reads what has arrived without blocking, and queues the answers that are complete. False on a stop request.
    bool continueReceiving(Process& process, const std::stop_token& stopToken);
    /// Moves to the next step once the current one has arrived. False on a stop request.
    bool finishStep(Process& process, const std::stop_token& stopToken);
    static void expect(Process& process, Step step, std::span<std::byte> bytes);
    /// Gets the buffer for the answer to the oldest batch of `process`, and expects its body there. False on a stop request.
    bool expectBody(Process& process, uint64_t bodySize, const std::stop_token& stopToken);
    /// Points the header of the output buffer at the body that arrived, after validating it.
    void finishRawBatch(Process& process) const;
    void finishIpcBatch(Process& process) const;
    void setDecoderSchema(Process& process, const ArrowSchema& received) const;

    PythonProcessChannel::Lease channel;
    std::vector<UdfProcessCall> calls;
    UdfProcessProtocol protocol;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::shared_ptr<ArrowBufferRef> layout;
    nanoarrow::UniqueSchema arrowSchema;
    std::vector<std::unique_ptr<Process>> processes;

    /// Answers not passed on yet.
    std::deque<Result> results;
    /// A buffer the source loop handed `fillTupleBuffer` and that no answer took yet, used for the next one.
    std::optional<TupleBuffer> spare;
    bool channelEnded = false;
};

}
