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

#include <API/Schema.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Runtime/Execution/DataEmitter.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/GatheringMode.hpp>
#include <atomic>
#include <chrono>
#include <folly/Synchronized.h>
#include <future>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

namespace NES::Runtime::MemoryLayouts {
class TestTupleBuffer;
}

namespace NES {
class KalmanFilter;

/**
* @brief Base class for all data sources in NES
* we allow only three cases:
*  1.) If the user specifies a numBuffersToProcess:
*      1.1) if the source e.g. file is large enough we will read in numBuffersToProcess and then terminate
*      1.2) if the file is not large enough, we will start at the beginning until we produced numBuffersToProcess
*  2.) If the user set numBuffersToProcess to 0, we read the source until it ends, e.g, until the file ends
*  3.) If the user just set numBuffersToProcess to n but does not say how many tuples he wants per buffer, we loop over the source until the buffer is full
*/

class DataSource : public Runtime::Reconfigurable, public DataEmitter {

  public:
    /**
     * @brief public constructor for data source
     * @Note the number of buffers to process is set to UINT64_MAX and the value is needed
     * by some test to produce a deterministic behavior
     * @param schema of the data that this source produces
     * @param bufferManager pointer to the buffer manager
     * @param queryManager pointer to the query manager
     * @param operatorId current operator id
     * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
     * @param statisticId represents the unique identifier of components that we can track statistics for
     * @param numSourceLocalBuffers number of local source buffers
     * @param gatheringMode the gathering mode (INTERVAL_MODE, INGESTION_RATE_MODE, or ADAPTIVE_MODE)
     * @param physicalSourceName the name and unique identifier of a physical source
     * @param persistentSource if the source has to persist properties that can be loaded when restarting the source
     * @param successors the subsequent operators in the pipeline to which the data is pushed
     * @param sourceAffinity the subsequent operators in the pipeline to which the data is pushed
     * @param taskQueueId the ID of the queue to which the task is pushed
     */
    explicit DataSource(SchemaPtr schema,
                        Runtime::BufferManagerPtr bufferManager,
                        Runtime::QueryManagerPtr queryManager,
                        OperatorId operatorId,
                        OriginId originId,
                        StatisticId statisticId,
                        size_t numSourceLocalBuffers,
                        GatheringMode gatheringMode,
                        const std::string& physicalSourceName,
                        bool persistentSource,
                        std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors =
                            std::vector<Runtime::Execution::SuccessorExecutablePipeline>(),
                        uint64_t sourceAffinity = std::numeric_limits<uint64_t>::max(),
                        uint64_t taskQueueId = 0);

    DataSource() = delete;

    /**
     * @brief This methods initializes thread-local state. For instance, it creates the local buffer pool and is necessary
     * because we cannot do it in the constructor.
     */
    virtual void open();

    bool injectEpochBarrier(uint64_t epochBarrier) const;

    /**
     * @brief This method cleans up thread-local state for the source.
     */
    virtual void close();

    /**
     * @brief method to start the source.
     * 1.) check if bool running is true, if true return if not start source
     * 2.) start new thread with runningRoutine
     */
    virtual bool start();

    /**
     * @brief method to stop the source.
     * 1.) check if bool running is false, if false return, if not stop source
     * 2.) stop thread by join
     */
    [[nodiscard]] virtual bool stop(Runtime::QueryTerminationType graceful);

    /**
     * @brief running routine while source is active
     */
    virtual void runningRoutine();

    /**
     * @brief virtual function to receive a buffer
     * @Note this function is overwritten by the particular data source
     * @return returns a tuple buffer
     */
    virtual std::optional<Runtime::TupleBuffer> receiveData() = 0;

    /**
     * @brief This method defines the logic to load the properties persisted during the previous run of the data source
     */
    virtual void createOrLoadPersistedProperties();

    /**
     * @brief This method stores all properties that need to be persisted for future retrieval during createOrLoadPersistedProperties call
     */
    virtual void storePersistedProperties();

    /**
     * @brief This method clears the persisted properties of the data source on had stop.
     */
    virtual void clearPersistedProperties();

    /**
     * @brief virtual function to get a string describing the particular source
     * @Note this function is overwritten by the particular data source
     * @return string with name and additional information about the source
     */
    virtual std::string toString() const = 0;

    /**
     * @brief get source Type
     * @return
     */
    virtual SourceType getType() const = 0;

    /**
     * @brief method to return the current schema of the source
     * @return schema description of the source
     */
    SchemaPtr getSchema() const;

    /**
     * @brief method to return the current schema of the source as string
     * @return schema description of the source as string
     */
    std::string getSourceSchemaAsString();

    /**
     * @brief debug function for testing to test if source is running
     * @return bool indicating if source is running
     * @dev    I made this function non-virtual. If implementations of this class should be able to override
     *         this function, we have to ensure that `isRunning` and this class' private member `running` are
     *         consistent or that this class does not evaluate `running` directly when checking if it is running.
     */
    inline bool isRunning() const noexcept { return running; }

    /**
     * @brief debug function for testing to get number of generated tuples
     * @return number of generated tuples
     */
    uint64_t getNumberOfGeneratedTuples() const;

    /**
     * @brief debug function for testing to get number of generated buffer
     * @return number of generated buffer
     */
    uint64_t getNumberOfGeneratedBuffers() const;

    /**
     * @brief This method will mark the running as false this will result in 1. the interruption of data gathering loop
     * and 2. initiation of soft stop routine.
     * @return true if successfully interrupted else false
     */
    virtual bool performSoftStop();

    /**
     * @brief method to set the sampling interval
     * @note the source will sleep for interval seconds and then produce the next buffer
     * @param interal to gather
     */
    void setGatheringInterval(std::chrono::milliseconds interval);

    /**
     * @brief Internal destructor to make sure that the data source is stopped before deconstrcuted
     * @Note must be public because of boost serialize
     */
    virtual ~DataSource() NES_NOEXCEPT(false) override;

    /**
     * @brief Get number of buffers to be processed
     */
    uint64_t getNumBuffersToProcess() const;

    /**
     * @brief Get gathering interval
     */
    std::chrono::milliseconds getGatheringInterval() const;

    /**
     * @brief Get number representation of gathering interval
     */
    uint64_t getGatheringIntervalCount() const;

    /**
     * @brief Gets the operator id for the data source
     * @return OperatorId
     */
    OperatorId getOperatorId() const;

    /**
     * @brief Set the operator id for the data source
     * @param operatorId
     */
    void setOperatorId(OperatorId operatorId);

    /**
     * @brief Returns the list of successor pipelines.
     * @return  std::vector<Runtime::Execution::SuccessorExecutablePipeline>
     */
    std::vector<Runtime::Execution::SuccessorExecutablePipeline> getExecutableSuccessors();

    /**
     * @brief Add a list of successor pipelines.
     */
    void addExecutableSuccessors(std::vector<Runtime::Execution::SuccessorExecutablePipeline> newPipelines);

    /**
     * @brief This method is necessary to avoid problems with the shared_from_this machinery combined with multi-inheritance
     * @tparam Derived the class type that we want to cast the shared ptr
     * @return this instance casted to the desired shared_ptr<Derived> type
     */
    template<typename Derived>
    std::shared_ptr<Derived> shared_from_base() {
        return std::static_pointer_cast<Derived>(DataEmitter::shared_from_this());
    }

    /**
     * @brief This method returns all supported layouts.
     * @return
     */
    virtual std::vector<Schema::MemoryLayoutType> getSupportedLayouts();

    /**
     * @brief API method called upon receiving an event.
     * @note Currently has no behaviour. We need to overwrite DataEmitter::onEvent for compliance.
     * @param event
     */
    virtual void onEvent(Runtime::BaseEvent&) override;
    /**
     * @brief API method called upon receiving an event, whose handling requires the WorkerContext (e.g. its network channels).
     * @note Only calls onEvent(event) of this class or derived classes.
     * @param event
     * @param workerContext
     */
    virtual void onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef workerContext);

    [[nodiscard]] virtual bool fail();

    /**
     * @brief set source sharing value
     * @param value
     */
    void setSourceSharing(bool value) { sourceSharing = value; };

    /**
     * @brief set the number of queries that use this source
     * @param value
     */
    void incrementNumberOfConsumerQueries() { numberOfConsumerQueries++; };

    /**
     * @brief check if a reconfiguration marker contains an event for this source. If so, trigger the reconfiguration and
     * propagate the marker downstream.
     * @param marker a marker containing a set of reconfiguration events
     * @return true if a reconfiguration was triggered, false if the marker did not contain any event to be handled by this source
     */
    virtual bool handleReconfigurationMarker(ReconfigurationMarkerPtr marker);

    /**
     * @brief Schedule a list of new executable successor to which the data strem is redirected when the old plan is drained and
     * a new plan is started and reuses this source.
     */
    void scheduleSuccessors(std::vector<Runtime::Execution::SuccessorExecutablePipeline> newSuccessorList);

    /**
     * @brief Compare the operator ids of two data sources
     * @return true if the ids match
     */
    static bool compareId(const DataSourcePtr& first, const DataSourcePtr& second);

    /**
     * @brief How swaps the successors of a source. This shuts down the plan to which a source currently belongs and connects
     * the source to the scheduled executable successors belonging to the new plan.
     * @param marker the marker to propagate to the old successor pipelines
     * @param sourceId the id of the source to reuse
     * @param newPlans the new plans to which the source belongs after the data stream is redirected
     */
    bool updateSuccessors(const ReconfigurationMarkerPtr& marker,
                          OperatorId sourceId,
                          std::vector<Runtime::Execution::ExecutableQueryPlanPtr> newPlans);

    // bool indicating if the data source has to persist runtime properties
    // that can be loaded during the restart of the query.
    const bool persistentSource;

    // Key to identify the source when loading persisted properties
    const std::string persistentSourceKey;

  protected:
    void emitWork(Runtime::TupleBuffer& buffer, bool addBufferMetaData = true) override;

    NES::Runtime::MemoryLayouts::TestTupleBuffer allocateBuffer();

    /*
     * @brief lock this sources executable successors and notify source completion.
     * By locking the successors it is ensured that the source to qep mapping is not modified while this function executes
     * @param terminationType the termination type with which the source completed
     */
    void lockSuccessorsAndNotifySourceCompletion(Runtime::QueryTerminationType terminationType);

    Runtime::QueryManagerPtr queryManager;
    Runtime::BufferManagerPtr localBufferManager;
    Runtime::FixedSizeBufferPoolPtr bufferManager{nullptr};
    folly::Synchronized<std::vector<Runtime::Execution::SuccessorExecutablePipeline>> executableSuccessors;
    folly::Synchronized<std::queue<std::vector<Runtime::Execution::SuccessorExecutablePipeline>>> newSuccessors;
    OperatorId operatorId;
    OriginId originId;
    StatisticId statisticId;
    SchemaPtr schema;
    uint64_t generatedTuples{0};
    uint64_t generatedBuffers{0};
    uint64_t numberOfBuffersToProduce = std::numeric_limits<decltype(numberOfBuffersToProduce)>::max();
    uint64_t numSourceLocalBuffers;
    uint64_t gatheringIngestionRate{};
    std::chrono::milliseconds gatheringInterval{0};
    GatheringMode gatheringMode;
    SourceType type;
    Runtime::QueryTerminationType wasGracefullyStopped{Runtime::QueryTerminationType::Graceful};// protected by mutex
    std::atomic_bool wasStarted{false};
    std::atomic_bool futureRetrieved{false};
    std::atomic_bool running{false};
    std::promise<bool> completedPromise;
    uint64_t sourceAffinity;
    uint64_t taskQueueId;
    bool sourceSharing = false;
    const std::string physicalSourceName;
    //this counter is used to count the number of queries that use this source
    std::atomic<uint64_t> refCounter = 0;
    std::atomic<uint64_t> numberOfConsumerQueries = 1;
    Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout;

  private:
    mutable std::recursive_mutex startStopMutex;
    uint64_t maxSequenceNumber = 0;

    mutable std::recursive_mutex successorModifyMutex;
    /**
    * @brief running routine with a fixed gathering interval
    */
    virtual void runningRoutineWithGatheringInterval();

    /**
    * @brief running routine with a fix ingestion rate
    */
    virtual void runningRoutineWithIngestionRate();

    bool endOfStreamSent{false};// protected by startStopMutex
    std::optional<ReconfigurationMarkerPtr> reconfigurationMarker = std::nullopt;
};

using DataSourcePtr = std::shared_ptr<DataSource>;

}// namespace NES

#endif// NES_RUNTIME_INCLUDE_SOURCES_DATASOURCE_HPP_
