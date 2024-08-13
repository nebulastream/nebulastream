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
#include <ostream>
#include <string>
#include <string_view>
#include <API/Schema.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/DataEmitter.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Sources/Source.hpp>

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
class DataSource : public Runtime::Reconfigurable
{
public:
    explicit DataSource(
        OriginId originId,
        SchemaPtr schema,
        Runtime::BufferManagerPtr bufferManager,
        Runtime::QueryManagerPtr queryManager,
        size_t numSourceLocalBuffers,
        std::unique_ptr<Source> sourceImplementation,
        uint64_t numberOfBuffersToProduce,
        const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& executableSuccessors
        = std::vector<Runtime::Execution::SuccessorExecutablePipeline>(),
        uint64_t taskQueueId = 0);

    DataSource() = delete;

    ~DataSource() NES_NOEXCEPT(false) override;

    /// clean up thread-local state for the source.
    void close();

    /// if not already running, start new thread with runningRoutine (finishes, when runningRoutine finishes)
    [[nodiscard]] bool start();

    /// check if bool running is false, if false return, if not stop source
    [[nodiscard]] bool stop(Runtime::QueryTerminationType graceful);

    [[nodiscard]] OriginId getOriginId() const;

    [[nodiscard]] std::string toString() const;

    const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& getExecutableSuccessors();

private:
    OriginId originId;
    SchemaPtr schema;
    Runtime::QueryManagerPtr queryManager;
    Runtime::BufferManagerPtr localBufferManager;
    Runtime::FixedSizeBufferPoolPtr bufferManager{nullptr};
    std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors;
    uint64_t numberOfBuffersToProduce = std::numeric_limits<decltype(numberOfBuffersToProduce)>::max();
    uint64_t numSourceLocalBuffers;
    SourceType type;
    Runtime::QueryTerminationType terminationType{Runtime::QueryTerminationType::Graceful}; /// protected by mutex
    std::atomic_bool wasStarted{false};
    std::atomic_bool futureRetrieved{false};
    std::atomic_bool running{false};
    std::promise<bool> completedPromise;
    uint64_t taskQueueId;
    Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout;
    uint64_t maxSequenceNumber = 0;
    std::unique_ptr<Source> sourceImplementation;
    mutable std::recursive_mutex startStopMutex;
    mutable std::recursive_mutex successorModifyMutex;
    bool endOfStreamSent{false}; /// protected by startStopMutex

    /// Runs in detached thread and kills thread when finishing.
    /// while (running) { ... }: orchestrates data ingestion until end of stream or failure.
    void runningRoutine();
    void emitWork(Runtime::TupleBuffer& buffer, bool addBufferMetaData = true);
    NES::Runtime::MemoryLayouts::TestTupleBuffer allocateBuffer();
};

} /// namespace NES

#endif /// NES_RUNTIME_INCLUDE_SOURCES_DATASOURCE_HPP_
