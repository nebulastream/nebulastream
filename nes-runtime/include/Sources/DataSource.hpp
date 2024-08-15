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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_DATASOURCE_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_DATASOURCE_HPP_

#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <string>
#include <API/Schema.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>

namespace NES::Runtime::MemoryLayouts
{
class TestTupleBuffer;
}

namespace NES
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
        OriginId originId,
        SchemaPtr schema,
        Runtime::BufferManagerPtr bufferManager,
        SourceReturnType::EmitFunction&&,
        size_t numSourceLocalBuffers,
        std::unique_ptr<Source> sourceImplementation,
        uint64_t numberOfBuffersToProduce);

    DataSource() = delete;

    ~DataSource() = default;

    /// clean up thread-local state for the source.
    void close();

    /// if not already running, start new thread with runningRoutine (finishes, when runningRoutine finishes)
    [[nodiscard]] bool start();

    /// check if bool running is false, if false return, if not stop source
    [[nodiscard]] bool stop();

    [[nodiscard]] OriginId getOriginId() const;

    friend std::ostream & operator<<(std::ostream& out, const DataSource& dataSource);

private:
    OriginId originId;
    SchemaPtr schema;
    Runtime::BufferManagerPtr localBufferManager;
    SourceReturnType::EmitFunction emitFunction;
    Runtime::FixedSizeBufferPoolPtr bufferManager{nullptr};
    uint64_t numberOfBuffersToProduce = std::numeric_limits<decltype(numberOfBuffersToProduce)>::max();
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
    void emitWork(Runtime::TupleBuffer& buffer, bool addBufferMetaData = true);
    [[nodiscard]] std::string toString() const;
    NES::Runtime::MemoryLayouts::TestTupleBuffer allocateBuffer() const;
};

} /// namespace NES

#endif /// NES_RUNTIME_INCLUDE_SOURCES_DATASOURCE_HPP_
