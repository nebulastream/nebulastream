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

#ifndef NES_CORE_INCLUDE_SOURCES_DATASOURCE_HPP_
#define NES_CORE_INCLUDE_SOURCES_DATASOURCE_HPP_

#include "Runtime/Execution/UnikernelPipelineExecutionContext.h"
#include "Sources/DataSource.hpp"// for NES_CORE_INCLUDE_SOURCES_DATASOURC...
#include <API/Schema.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Identifiers.hpp>
#include <Runtime/Execution/DataEmitter.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/GatheringMode.hpp>
#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <optional>
#include <thread>

namespace NES {

/**
    * @brief Base class for all data sources in NES
    * we allow only three cases:
    *  1.) If the user specifies a numBuffersToProcess:
    *      1.1) if the source e.g. file is large enough we will read in numBuffersToProcess and then terminate
    *      1.2) if the file is not large enough, we will start at the beginning until we produced numBuffersToProcess
    *  2.) If the user set numBuffersToProcess to 0, we read the source until it ends, e.g, until the file ends
    *  3.) If the user just set numBuffersToProcess to n but does not say how many tuples he wants per buffer, we loop over the source until the buffer is full
    */

class DataSource : public DataEmitter {

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
         * @param numSourceLocalBuffers number of local source buffers
         * @param gatheringMode the gathering mode (INTERVAL_MODE, INGESTION_RATE_MODE, or ADAPTIVE_MODE)
         * @param physicalSourceName the name and unique identifier of a physical source
         * @param successors the subsequent operators in the pipeline to which the data is pushed
         * @param sourceAffinity the subsequent operators in the pipeline to which the data is pushed
         * @param taskQueueId the ID of the queue to which the task is pushed
         */
    explicit DataSource(OperatorId operatorId,
                        OriginId originId,
                        size_t numSourceLocalBuffers,
                        const std::string& physicalSourceName,
                        NES::Unikernel::UnikernelPipelineExecutionContext successor,
                        uint64_t sourceAffinity = std::numeric_limits<uint64_t>::max(),
                        uint64_t taskQueueId = 0);

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
         * @brief Internal destructor to make sure that the data source is stopped before deconstrcuted
         * @Note must be public because of boost serialize
         */
    virtual ~DataSource() override;

    /**
         * @brief Get number of buffers to be processed
         */
    uint64_t getNumBuffersToProcess() const;

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

    /**
         * @brief method injects epoch barrier to the data source
         * @param epochBarrier current epoch barrier
         * @param queryId currect query id
         * @return success is the message was sent
         */
    virtual bool injectEpochBarrier(uint64_t epochBarrier, uint64_t queryId);

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

  protected:
    Runtime::BufferManagerPtr localBufferManager;
    Runtime::FixedSizeBufferPoolPtr bufferManager{nullptr};
    NES::Unikernel::UnikernelPipelineExecutionContext executableSuccessors;
    OperatorId operatorId;
    OriginId originId;
    SchemaPtr schema;
    uint64_t generatedTuples{0};
    uint64_t generatedBuffers{0};
    uint64_t numberOfBuffersToProduce = std::numeric_limits<decltype(numberOfBuffersToProduce)>::max();
    uint64_t numSourceLocalBuffers;
    SourceType type;
    Runtime::QueryTerminationType wasGracefullyStopped{Runtime::QueryTerminationType::Graceful};// protected by mutex
    bool wasStarted{false};
    bool futureRetrieved{false};
    bool running{false};
    uint64_t sourceAffinity;
    uint64_t taskQueueId;
    bool sourceSharing = false;
    std::string physicalSourceName;

    //this counter is used to count the number of queries that use this source
    uint64_t refCounter = 0;
    uint64_t numberOfConsumerQueries = 1;

    /**
         * @brief Emits a tuple buffer to the successors.
         * @param buffer
         */
    void emitWork(Runtime::TupleBuffer& buffer) override;

    void emitWorkFromSource(Runtime::TupleBuffer& buffer);

  protected:
  private:
    uint64_t maxSequenceNumber = 0;

    virtual void unikernelRunningRoutine();

    bool endOfStreamSent{false};// protected by startStopMutex
};

using DataSourcePtr = std::shared_ptr<DataSource>;

}// namespace NES

#endif// NES_CORE_INCLUDE_SOURCES_DATASOURCE_HPP_
