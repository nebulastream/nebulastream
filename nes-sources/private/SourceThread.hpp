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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <ostream>
#include <stop_token>
#include <thread>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/Formatter.hpp>
#include <magic_enum/magic_enum.hpp>

namespace NES::Sources
{
struct SourceImplementationTermination
{
    enum : uint8_t
    {
        StopRequested,
        EndOfStream
    } result;

    friend std::ostream& operator<<(std::ostream& os, const SourceImplementationTermination& obj)
    {
        return os << magic_enum::enum_name(obj.result);
    }
};

/// The sourceThread starts a detached thread that runs 'runningRoutine()' upon calling 'start()'.
/// The runningRoutine orchestrates data ingestion until an end of stream (EOS) or a failure happens.
/// The data source emits tasks into the TaskQueue when buffers are full, a timeout was hit, or a flush happens.
/// The data source can call 'addEndOfStream()' from the QueryManager to stop a query via a reconfiguration message.
class SourceThread
{
    static constexpr auto STOP_TIMEOUT_NOT_RUNNING = std::chrono::seconds(60);
    static constexpr auto STOP_TIMEOUT_RUNNING = std::chrono::seconds(300);

public:
    explicit SourceThread(
        OriginId originId, /// Todo #241: Rethink use of originId for sources, use new identifier for unique identification.
        std::shared_ptr<Memory::AbstractPoolProvider> bufferManager,
        size_t numSourceLocalBuffers,
        std::unique_ptr<Source> sourceImplementation);

    SourceThread() = delete;
    SourceThread(const SourceThread& other) = delete;
    SourceThread(SourceThread&& other) noexcept = delete;
    SourceThread& operator=(const SourceThread& other) = delete;
    SourceThread& operator=(SourceThread&& other) noexcept = delete;

    /// clean up thread-local state for the source.
    void close();

    /// if not already running, start new thread with runningRoutine (finishes, when runningRoutine finishes)
    [[nodiscard]] bool start(SourceReturnType::EmitFunction&& emitFunction);

    /// Blocks the current thread until the source is terminated
    void stop();


    /// Attempts to terminate the source within the timeout
    [[nodiscard]] SourceReturnType::TryStopResult tryStop(std::chrono::milliseconds timeout);

    /// Todo #241: Rethink use of originId for sources, use new identifier for unique identification.
    [[nodiscard]] OriginId getOriginId() const;

    friend std::ostream& operator<<(std::ostream& out, const SourceThread& sourceThread);

protected:
    OriginId originId;
    std::shared_ptr<Memory::AbstractPoolProvider> localBufferManager;
    uint64_t numSourceLocalBuffers;
    std::unique_ptr<Source> sourceImplementation;
    std::atomic_bool started;

    std::jthread thread;
    std::future<SourceImplementationTermination> terminationFuture;

    /// Runs in detached thread and kills thread when finishing.
    /// while (running) { ... }: orchestrates data ingestion until end of stream or failure.
    void runningRoutine(const std::stop_token& stopToken, std::promise<SourceImplementationTermination>&);
    void emitWork(NES::Memory::TupleBuffer& buffer, bool addBufferMetaData = true);
    friend std::ostream& operator<<(std::ostream& out, const SourceThread& sourceThread);
};

}

FMT_OSTREAM(NES::Sources::SourceImplementationTermination);
