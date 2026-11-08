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

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <exception>
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <sys/types.h>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <nanoarrow/nanoarrow.hpp>
#include <nanoarrow/nanoarrow_ipc.hpp>
#include <ErrorHandling.hpp>
#include <PythonProcessChannel.hpp>
#include <UdfProcessCall.hpp>

namespace NES
{

struct PythonProcessSourceConfig
{
    /// Id of the channel the `PythonProcess` sink of the same UDF operator hands its buffers to.
    std::string channel;
    /// The serialized calls of the UDF operator, see UdfProcessCall.
    std::string calls;

    static std::expected<PythonProcessSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Starts the pipeline behind an out-of-process UDF operator, and owns its processes: the worker's venv's python if one is configured,
/// otherwise python3 on the PATH, which needs pyarrow. Each process has a GIL of its own, so the source starts several of them, as
/// many as NES_UDF_PROCESSES says (worker.python_udf.processes), and a thread per process that takes batches from the channel, sends
/// one at a time and waits for it, so no worker thread ever waits for Python. Batches travel as Arrow IPC streams in both directions:
/// the thread encodes its input with nanoarrow, the process reads and writes them with pyarrow and answers with the input columns plus
/// the results, and the thread points the header of an output buffer at the body of the answer. The source thread passes the output
/// buffers on with the metadata of their input, in the order the processes finish them.
class PythonProcessSource final : public Source
{
public:
    static constexpr std::string_view NAME = "PythonProcess";

    PythonProcessSource(const PythonProcessSourceConfig& config, const SourceDescriptor& sourceDescriptor);
    ~PythonProcessSource() override;
    PythonProcessSource(const PythonProcessSource&) = delete;
    PythonProcessSource& operator=(const PythonProcessSource&) = delete;
    PythonProcessSource(PythonProcessSource&&) = delete;
    PythonProcessSource& operator=(PythonProcessSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    /// Origin id, sequence and chunk number and watermark are taken over from the input.
    [[nodiscard]] bool addsMetadata() const override { return true; }

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// A Python process with the IPC stream state of the batches it exchanges.
    struct Process
    {
        pid_t pid = -1;
        int socket = -1;
        nanoarrow::ipc::UniqueEncoder encoder;
        nanoarrow::ipc::UniqueDecoder decoder;
    };

    /// A batch a process has finished, or the failure of one.
    struct Result
    {
        TupleBuffer input;
        std::optional<TupleBuffer> output;
        std::exception_ptr failure;
    };

    void startProcess(Process& process) const;
    /// Exchanges batches from the channel with `process` and queues the results, until the channel ends or a stop is requested.
    void serve(Process& process, const std::stop_token& stopToken);
    /// Return false on a stop request.
    bool pushResult(Result result, const std::stop_token& stopToken);
    static bool sendAll(const Process& process, std::span<const std::byte> bytes, const std::stop_token& stopToken);
    static bool receiveAll(const Process& process, std::span<std::byte> bytes, const std::stop_token& stopToken);
    static bool receiveStatus(const Process& process, const std::stop_token& stopToken);
    bool sendBatch(Process& process, const TupleBuffer& input, const std::stop_token& stopToken) const;
    bool receiveBatch(Process& process, TupleBuffer& output, uint64_t rows, const std::stop_token& stopToken) const;
    /// Reads the next IPC message into `message`; false on a stop request.
    static bool receiveMessage(const Process& process, std::vector<std::byte>& message, const std::stop_token& stopToken);
    void setDecoderSchema(Process& process, const ArrowSchema& received) const;

    PythonProcessChannel::Lease channel;
    std::vector<UdfProcessCall> calls;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::shared_ptr<ArrowBufferRef> layout;
    nanoarrow::UniqueSchema arrowSchema;
    std::vector<std::unique_ptr<Process>> processes;

    std::mutex resultsMutex;
    std::condition_variable_any resultsChanged;
    std::deque<Result> results;
    /// The threads that serve a process and have not ended yet.
    size_t serving = 0;
    /// Declared last, so that they are joined before anything they use is destroyed.
    std::vector<std::jthread> servers;
};

}
