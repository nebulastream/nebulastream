/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_SOURCES_DATA_SOURCE_HPP_
#define NES_INCLUDE_SOURCES_DATA_SOURCE_HPP_

#include <API/Schema.hpp>
#include <Operators/OperatorId.hpp>
#include <Runtime/Execution/DataEmitter.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <thread>
#ifdef ENABLE_ADAPTIVE_SAMPLING
#include <Util/KalmanFilter.hpp>
#endif

namespace NES {

enum SourceType {
    OPC_SOURCE,
    ZMQ_SOURCE,
    CSV_SOURCE,
    KAFKA_SOURCE,
    TEST_SOURCE,
    BINARY_SOURCE,
    SENSE_SOURCE,
    DEFAULT_SOURCE,
    NETWORK_SOURCE,
    MONITORING_SOURCE,
    MEMORY_SOURCE,
    MQTT_SOURCE,
    LAMBDA_SOURCE
};

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
    enum GatheringMode { FREQUENCY_MODE, INGESTION_RATE_MODE, ADAPTIVE_MODE };

    /**
     * @brief public constructor for data source
     * @Note the number of buffers to process is set to UINT64_MAX and the value is needed
     * by some test to produce a deterministic behavior
     * @param schema of the data that this source produces
     */
    explicit DataSource(SchemaPtr schema,
                        Runtime::BufferManagerPtr bufferManager,
                        Runtime::QueryManagerPtr queryManager,
                        OperatorId operatorId,
                        size_t numSourceLocalBuffers,
                        GatheringMode gatheringMode,
                        std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors =
                            std::vector<Runtime::Execution::SuccessorExecutablePipeline>());

    DataSource() = delete;

    /**
     * @brief This methods initializes thread-local state. For instance, it creates the local buffer pool and is necessary
     * because we cannot do it in the constructor.
     */
    virtual void open();

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
    virtual bool stop(bool graceful);

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
     * @brief method to set the sampling interval
     * @note the source will sleep for interval seconds and then produce the next buffer
     * @param interal to gather
     */
    void setGatheringInterval(std::chrono::milliseconds interval);

    /**
     * @brief Internal destructor to make sure that the data source is stopped before deconstrcuted
     * @Note must be public because of boost serialize
     */
    ~DataSource() NES_NOEXCEPT(false) override;

    /**
     * @brief Get number of buffers to be processed
     */
    uint64_t getNumBuffersToProcess() const;

    /**
     * @brief Get frequency of gathering the data
     */
    std::chrono::milliseconds getGatheringInterval() const;

    /**
     * @brief Get frequency of gathering the data
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

    static GatheringMode getGatheringModeFromString(const std::string& mode);

    /**
     * @brief Returns the list of successor pipelines.
     * @return  std::vector<Runtime::Execution::SuccessorExecutablePipeline>
     */
    std::vector<Runtime::Execution::SuccessorExecutablePipeline> getExecutableSuccessors();

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
     * @brief This function checks if the layout of schema can be found in the implementation of getSupportedLayouts()
     * @param schema
     * @return
     */
    bool checkSupportedLayoutTypes(SchemaPtr& schema);

  protected:
    Runtime::QueryManagerPtr queryManager;
    Runtime::BufferManagerPtr localBufferManager;
    Runtime::FixedSizeBufferPoolPtr bufferManager{nullptr};
    std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors;
    OperatorId operatorId;
    SchemaPtr schema;
    uint64_t generatedTuples{0};
    uint64_t generatedBuffers{0};
    uint64_t numBuffersToProcess = std::numeric_limits<decltype(numBuffersToProcess)>::max();
    uint64_t numSourceLocalBuffers;
    uint64_t gatheringIngestionRate{};
    uint64_t sourceAffinity{std::numeric_limits<uint64_t>::max()};
    std::chrono::milliseconds gatheringInterval{0};
    GatheringMode gatheringMode;
    SourceType type;
    std::atomic<bool> wasGracefullyStopped{true};
    std::atomic_bool running{false};
    uint32_t numaNode;
    /**
     * @brief Emits a tuple buffer to the successors.
     * @param buffer
     */
    void emitWork(Runtime::TupleBuffer& buffer) override;

    void emitWorkFromSource(Runtime::TupleBuffer& buffer);
    Runtime::MemoryLayouts::DynamicTupleBuffer allocateBuffer();

  private:
    mutable std::mutex startStopMutex;
    std::shared_ptr<std::thread> thread{nullptr};
    uint64_t maxSequenceNumber = 0;
    Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout;

    /**
    * @brief running routine with a fix frequency
    */
    virtual void runningRoutineWithFrequency();

    /**
    * @brief running routine with a fix ingestion rate
    */
    virtual void runningRoutineWithIngestionRate();

    /**
    * @brief running routine with an adaptive rate (defaults to KF)
    */
    virtual void runningRoutineAdaptive();
#ifdef ENABLE_ADAPTIVE_SAMPLING
    // TODO: remove the ifdef in the project
    // https://github.com/nebulastream/nebulastream/issues/2400
    /**
     * @brief the KF associated with a source.
     * We use default values for initialization.
     */
    KalmanFilter kFilter;
#endif
};

using DataSourcePtr = std::shared_ptr<DataSource>;

}// namespace NES

#endif// NES_INCLUDE_SOURCES_DATA_SOURCE_HPP_
