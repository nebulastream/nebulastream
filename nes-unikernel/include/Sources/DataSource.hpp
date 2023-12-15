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

#include <API/Schema.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Identifiers.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/DataEmitter.hpp>
#include <Runtime/Execution/UnikernelPipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/WorkerContext.hpp>
#include <SchemaBuffer.hpp>
#include <Util/GatheringMode.hpp>
#include <Util/ThreadNaming.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

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
template<typename Config>
class DataSource : public DataEmitter {

  public:
    /**
         * @brief public constructor for data source
         * @Note the number of buffers to process is set to UINT64_MAX and the value is needed
         * by some test to produce a deterministic behavior
         * @param successors the subsequent operators in the pipeline to which the data is pushed
         */
    explicit DataSource(NES::Unikernel::UnikernelPipelineExecutionContext successor)
        : executableSuccessors(std::move(successor)){};

    DataSource() = delete;

    virtual ~DataSource() = default;

    using BufferType = NES::Unikernel::SchemaBuffer<typename Config::Schema, 8192>;

    /**
         * @brief This methods initializes thread-local state. For instance, it creates the local buffer pool and is necessary
         * because we cannot do it in the constructor.
         */
    virtual void open() { bufferManager = TheBufferManager->createFixedSizeBufferPool(numSourceLocalBuffers); }

    /**
         * @brief This method cleans up thread-local state for the source.
         */
    virtual void close() {
        Runtime::QueryTerminationType queryTerminationType;
        { queryTerminationType = this->wasGracefullyStopped; }
        if (queryTerminationType != Runtime::QueryTerminationType::Graceful) {
            // inject reconfiguration task containing end of stream
            NES_ASSERT2_FMT(!endOfStreamSent, "Eos was already sent for source " << toString());
            NES_DEBUG("DataSource {} : Data Source add end of stream. Gracefully={}", Config::OperatorId, queryTerminationType);

            bufferManager->destroy();
        }
    }

    /**
         * @brief method to start the source.
         * 1.) check if bool running is true, if true return if not start source
         * 2.) start new thread with runningRoutine
         */
    virtual bool start() {
        NES_DEBUG("DataSource  {} : start source", Config::OperatorId);
        if (running)
            return false;

        running = true;
        return true;
    }

    /**
         * @brief method to stop the source.
         * 1.) check if bool running is false, if false return, if not stop source
         * 2.) stop thread by join
         */
    [[nodiscard]] virtual bool stop(Runtime::QueryTerminationType graceful);

    /**
         * @brief running routine while source is active
         */
    virtual void runningRoutine() {
        static_assert(Config::OperatorId != 0, "The id of the source is not set properly");
        std::string thName = "DataSrc-" + std::to_string(Config::OperatorId);
        setThreadName(thName.c_str());

        NES_DEBUG("DataSource {}: Running Data Source of type={} ",
                  Config::OperatorId,
                  magic_enum::enum_name(Config::SourceType::SourceType));

        if (numberOfBuffersToProduce == 0) {
            NES_DEBUG(
                "DataSource: the user does not specify the number of buffers to produce therefore we will produce buffer until "
                "the source is empty");
        } else {
            NES_DEBUG("DataSource: the user specify to produce {} buffers", numberOfBuffersToProduce);
        }
        open();
        uint64_t numberOfBuffersProduced = 0;
        while (running) {
            //check if already produced enough buffer
            if (numberOfBuffersToProduce == 0 || numberOfBuffersProduced < numberOfBuffersToProduce) {
                auto optBuf = receiveData();// note that receiveData might block
                if (!running) {// necessary if source stops while receiveData is called due to stricter shutdown logic
                    break;
                }
                //this checks we received a valid output buffer
                if (optBuf.has_value()) {
                    auto& buf = optBuf.value();
                    NES_TRACE("DataSource produced buffer {} type= {} string={}: Received Data: {} tuples iteration= {} "
                              "Config::OperatorId={} orgID={}",
                              Config::OperatorId,
                              magic_enum::enum_name(Config::SourceType::SourceType),
                              toString(),
                              buf.getNumberOfTuples(),
                              numberOfBuffersProduced,
                              Config::OperatorId,
                              Config::OperatorId);

                    emitWorkFromSource(buf);
                    ++numberOfBuffersProduced;
                } else {
                    NES_DEBUG("DataSource {}: stopping cause of invalid buffer", Config::OperatorId);
                    running = false;
                    NES_DEBUG("DataSource {}: Thread going to terminating with graceful exit.", Config::OperatorId);
                }
            } else {
                NES_DEBUG("DataSource {}: Receiving thread terminated ... stopping because cnt={} smaller than "
                          "numBuffersToProcess={} now return",
                          Config::OperatorId,
                          numberOfBuffersProduced,
                          numberOfBuffersToProduce);
                running = false;
            }
            NES_TRACE("DataSource {} : Data Source finished processing iteration {}",
                      Config::OperatorId,
                      numberOfBuffersProduced);
        }
        NES_DEBUG("DataSource {} call close", Config::OperatorId);
        close();
        NES_DEBUG("DataSource {} end running", Config::OperatorId);
    }

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
    uint64_t getNumberOfGeneratedTuples() const { return generatedTuples; }

    /**
         * @brief debug function for testing to get number of generated buffer
         * @return number of generated buffer
         */
    uint64_t getNumberOfGeneratedBuffers() const { return generatedBuffers; }

    /**
         * @brief Get number of buffers to be processed
         */
    uint64_t getNumBuffersToProcess() const { return numberOfBuffersToProduce; }

    /**
         * @brief API method called upon receiving an event.
         * @note Currently has no behaviour. We need to overwrite DataEmitter::onEvent for compliance.
         * @param event
         */
    void onEvent(Runtime::BaseEvent& event) override {
        NES_DEBUG("DataSource::onEvent(event) called. Config::OperatorId: {}", Config::OperatorId);
        // no behaviour needed, call onEvent of direct ancestor
        DataEmitter::onEvent(event);
    }

    /**
         * @brief API method called upon receiving an event, whose handling requires the WorkerContext (e.g. its network channels).
         * @note Only calls onEvent(event) of this class or derived classes.
         * @param event
         * @param workerContext
         */
    virtual void onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef) {
        NES_DEBUG("DataSource::onEvent(event, wrkContext) called. Config::OperatorId:  {}", Config::OperatorId);
        onEvent(event);
    }

    /**
         * @brief method injects epoch barrier to the data source
         * @param epochBarrier current epoch barrier
         * @param queryId currect query id
         * @return success is the message was sent
         */
    virtual bool injectEpochBarrier(uint64_t epochBarrier, uint64_t queryId) {
        NES_DEBUG("DataSource::injectEpochBarrier received timestamp  {} with queryId  {}", epochBarrier, queryId);
        return true;
    }

    [[nodiscard]] virtual bool fail() {
        bool isStopped = stop(Runtime::QueryTerminationType::Failure);// this will block until the thread is stopped
        NES_DEBUG("Source {} stop executed= {}", Config::OperatorId, (isStopped ? "stopped" : "cannot stop"));
        {
            NES_DEBUG("Source {} has already injected failure? {}",
                      Config::OperatorId,
                      (endOfStreamSent ? "EoS sent" : "cannot send EoS"));
            if (!this->endOfStreamSent) {
            }
            return isStopped && endOfStreamSent;
        }
        return false;
    }

  protected:
    Runtime::FixedSizeBufferPoolPtr bufferManager{nullptr};
    NES::Unikernel::UnikernelPipelineExecutionContext executableSuccessors;
    uint64_t generatedTuples{0};
    uint64_t generatedBuffers{0};
    uint64_t numberOfBuffersToProduce = std::numeric_limits<decltype(numberOfBuffersToProduce)>::max();
    uint64_t numSourceLocalBuffers;
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
    void emitWork(Runtime::TupleBuffer& buffer) override { executableSuccessors.emit(buffer); }

    BufferType allocateBuffer() { return BufferType::of(TheBufferManager->getBufferBlocking()); }

    void emitWorkFromSource(Runtime::TupleBuffer& buffer) {
        {
            // set the origin id for this source
            buffer.setOriginId(Config::OriginId);
            // set the creation timestamp
            buffer.setCreationTimestampInMS(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                std::chrono::high_resolution_clock::now().time_since_epoch())
                                                .count());
            // Set the sequence number of this buffer.
            // A data source generates a monotonic increasing sequence number
            maxSequenceNumber++;
            buffer.setSequenceNumber(maxSequenceNumber);
            emitWork(buffer);
        }
    }

  protected:
  private:
    uint64_t maxSequenceNumber = 0;

    bool endOfStreamSent{false};// protected by startStopMutex
};

template<typename Config>
using DataSourcePtr = std::shared_ptr<DataSource<Config>>;

}// namespace NES

#endif// NES_CORE_INCLUDE_SOURCES_DATASOURCE_HPP_
