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

template<typename T, typename Config>
concept DataSourceImpl = requires(T t) {
    {
        t.receiveData(std::declval<const std::stop_token&>(),
                      std::declval<NES::Unikernel::SchemaBuffer<typename Config::Schema, 8192>>())
    } -> std::same_as<std::optional<NES::Unikernel::SchemaBuffer<typename Config::Schema, 8192>>>;

    { t.open() };
    { t.close() };
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
template<typename Config, DataSourceImpl<Config> Impl>
class DataSource {

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

    ~DataSource() = default;

    using BufferType = NES::Unikernel::SchemaBuffer<typename Config::Schema, 8192>;

    /**
     * @brief This methods initializes thread-local state. For instance, it creates the local buffer pool and is necessary
     * because we cannot do it in the constructor.
     */
    void open() {
        std::cerr << "DATA Open" << std::endl;
        NES_INFO("Data source open");
        bufferManager = TheBufferManager->createFixedSizeBufferPool(Config::LocalBuffers);
        impl.open();
    }

    /**
     * @brief This method cleans up thread-local state for the source.
     */
    void close() {
        impl.close();
        bufferManager->destroy();
    }

    /**
     * @brief method to start the source.
     * 1.) check if bool running is true, if true return if not start source
     * 2.) start new thread with runningRoutine
     */
    bool start() {
        NES_DEBUG("DataSource  {} : start source", Config::OperatorId);
        worker.emplace([this](const std::stop_token& stoken) {
            try {
                NES_DEBUG("DataSource {} call open", Config::OperatorId);
                open();
                NES_DEBUG("DataSource {} start running routine", Config::OperatorId);
                runningRoutine(stoken);
                NES_DEBUG("DataSource {} call close", Config::OperatorId);
                std::cerr << "About to close" << std::endl;
                close();
                NES_DEBUG("DataSource {} end running", Config::OperatorId);

                if (!stoken.stop_requested()) {
                    executableSuccessors.stop();
                }
            } catch (const std::exception& exception) {
                NES_ERROR("Exception: {}", exception.what());
            }
        });

        return true;
    }

    /**
     * @brief running routine while source is active
     */
    void runningRoutine(const std::stop_token& stoken) {
        static_assert(Config::OperatorId != 0, "The id of the source is not set properly");
        setThreadName(fmt::format("DataSrc-{}", Config::OperatorId).c_str());

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
        uint64_t numberOfBuffersProduced = 0;
        while (!stoken.stop_requested()) {
            //check if already produced enough buffer
            if (numberOfBuffersToProduce == 0 || numberOfBuffersProduced < numberOfBuffersToProduce) {
                auto buffer = allocateBuffer();
                auto optBuf = impl.receiveData(stoken, BufferType::of(buffer));
                if (stoken.stop_requested() && !optBuf.has_value()) {
                    // necessary if source stops while receiveData is called due to stricter shutdown logic
                    break;
                }
                //this checks we received a valid output buffer
                if (optBuf.has_value()) {
                    auto buf = optBuf.value().getBuffer();
                    NES_TRACE("DataSource produced buffer {} type= {}: Received Data: {} tuples iteration= {} "
                              "Config::OperatorId={} orgID={}",
                              Config::OperatorId,
                              magic_enum::enum_name(Config::SourceType::SourceType),
                              buf.getNumberOfTuples(),
                              numberOfBuffersProduced,
                              Config::OperatorId,
                              Config::OperatorId);

                    emitWorkFromSource(buf);
                    ++numberOfBuffersProduced;
                } else {
                    NES_DEBUG("DataSource {}: stopping cause of invalid buffer", Config::OperatorId);
                    NES_DEBUG("DataSource {}: Thread going to terminating with graceful exit.", Config::OperatorId);
                    break;
                }
            } else {
                NES_DEBUG("DataSource {}: Receiving thread terminated ... stopping because cnt={} smaller than "
                          "numBuffersToProcess={} now return",
                          Config::OperatorId,
                          numberOfBuffersProduced,
                          numberOfBuffersToProduce);
            }
            std::cerr << "About to close 1" << std::endl;
            NES_TRACE("DataSource {} : Data Source finished processing iteration {}",
                      Config::OperatorId,
                      numberOfBuffersProduced);
        }

        std::cerr << "About to close 2" << stoken.stop_requested() << std::endl;
    }

    /**
     * @brief debug function for testing to get number of generated tuples
     * @return number of generated tuples
     */
    [[nodiscard]] uint64_t getNumberOfGeneratedTuples() const { return generatedTuples; }

    /**
     * @brief debug function for testing to get number of generated buffer
     * @return number of generated buffer
     */
    [[nodiscard]] uint64_t getNumberOfGeneratedBuffers() const { return generatedBuffers; }

    /**
     * @brief Get number of buffers to be processed
     */
    [[nodiscard]] uint64_t getNumBuffersToProcess() const { return numberOfBuffersToProduce; }

  protected:
    std::optional<std::jthread> worker;
    Runtime::FixedSizeBufferPoolPtr bufferManager{nullptr};
    NES::Unikernel::UnikernelPipelineExecutionContext executableSuccessors;
    uint64_t generatedTuples{0};
    uint64_t generatedBuffers{0};
    uint64_t numberOfBuffersToProduce = std::numeric_limits<decltype(numberOfBuffersToProduce)>::max();

    /**
     * @brief Emits a tuple buffer to the successors.
     * @param buffer
     */
    void emitWork(Runtime::TupleBuffer& buffer) { executableSuccessors.emit(buffer); }

    NES::Runtime::TupleBuffer allocateBuffer() { return bufferManager->getBufferBlocking(); }

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

    uint64_t maxSequenceNumber = 0;
    Impl impl;
};

template<typename Config, DataSourceImpl<Config> Impl>
using DataSourcePtr = std::shared_ptr<DataSource<Config, Impl>>;

}// namespace NES

#endif// NES_CORE_INCLUDE_SOURCES_DATASOURCE_HPP_
