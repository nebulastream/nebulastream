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
#include <future>
#include <mutex>
#include <string>
#include <API/Schema.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>

namespace NES::Runtime::MemoryLayouts
{
class TestTupleBuffer;
}

namespace NES::Sources
{

/// The DataSource starts a detached thread that runs 'runningRoutine()' upon calling 'start()'.
/// The runningRoutine orchestrates data ingestion until an end of stream (EOS) or a failure happens.
/// The data source emits tasks into the TaskQueue when buffers are full, a timeout was hit, or a flush happens.
/// The data source can call 'addEndOfStream()' from the QueryManager to stop a query via a reconfiguration message.
class DataSource
{
    static constexpr auto STOP_TIMEOUT_NOT_RUNNING = std::chrono::seconds(60);
    static constexpr auto STOP_TIMEOUT_RUNNING = std::chrono::seconds(300);

public:
    explicit DataSource(
        OriginId originId, /// Todo #241: Rethink use of originId for sources, use new identifier for unique identification.
        SchemaPtr schema,
        std::shared_ptr<NES::Runtime::AbstractPoolProvider> bufferManager,
        SourceReturnType::EmitFunction&&,
        size_t numSourceLocalBuffers,
        std::unique_ptr<Source> sourceImplementation);

    DataSource() = delete;

    ~DataSource() = default;

    /// clean up thread-local state for the source.
    void close();

    /// if not already running, start new thread with runningRoutine (finishes, when runningRoutine finishes)
    [[nodiscard]] bool start();

    /// check if bool running is false. If running is false return, otherwise stops the source.
    [[nodiscard]] bool stop();

    /// Todo #241: Rethink use of originId for sources, use new identifier for unique identification.
    [[nodiscard]] OriginId getOriginId() const;

    friend std::ostream& operator<<(std::ostream& out, const DataSource& dataSource);

protected:
    OriginId originId;
    SchemaPtr schema;
    std::shared_ptr<NES::Runtime::AbstractPoolProvider> localBufferManager;
    SourceReturnType::EmitFunction emitFunction;
    std::shared_ptr<NES::Runtime::AbstractBufferProvider> bufferProvider{nullptr};
    uint64_t numSourceLocalBuffers;
    SourceType type;
    std::atomic_bool wasStarted{false};
    std::atomic_bool futureRetrieved{false};
    std::atomic_bool running{false};
    std::promise<bool> completedPromise;
    uint64_t maxSequenceNumber = 0;
    std::unique_ptr<Source> sourceImplementation;
    mutable std::recursive_mutex startStopMutex;
    mutable std::recursive_mutex successorModifyMutex;
    std::unique_ptr<std::thread> thread{nullptr};

    /// Runs in detached thread and kills thread when finishing.
    /// while (running) { ... }: orchestrates data ingestion until end of stream or failure.
    void runningRoutine();
    void emitWork(NES::Runtime::TupleBuffer& buffer, bool addBufferMetaData = true);
    [[nodiscard]] std::string toString() const;
    NES::Runtime::MemoryLayouts::TestTupleBuffer allocateBuffer() const;
};

}
