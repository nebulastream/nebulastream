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

#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/UnikernelPipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/WorkerContext.hpp>
#include <SchemaBuffer.hpp>
#include <Util/GatheringMode.hpp>
#include <Util/ThreadNaming.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <atomic>
#include <chrono>
#include <optional>
#include <thread>
#include <utility>

namespace NES {

template<typename T>
concept DataSourceConfig = requires(T* t) {
    requires(std::same_as<decltype(T::OperatorId), const size_t>);
    requires(std::same_as<decltype(T::OriginId), const size_t>);
    requires(std::same_as<decltype(T::LocalBuffers), const size_t>);
    typename T::Schema;
    typename T::SourceType;
};

template<typename T, typename Config>
concept DataSourceImpl = requires(T t) {
    { T::NeedsBuffer } -> std::same_as<const bool&>;
    {
        t.receiveData(std::declval<const std::stop_token&>(),
                      std::declval<NES::Unikernel::SchemaBuffer<typename Config::Schema, 8192>>())
    } -> std::same_as<std::optional<NES::Runtime::TupleBuffer>>;

    std::constructible_from<T>;
    { T::SourceType } -> std::same_as<const NES::SourceType&>;
    { t.open() };
    { t.close(std::declval<Runtime::QueryTerminationType>()) };
};

template<typename T>
concept BufferLimitingConfig = requires(T t) {
    DataSourceConfig<T>;
    { T::NumbersOfBufferToProduce } -> std::same_as<size_t>;
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
     * @brief method to start the source.
     * 1.) start new thread with runningRoutine
     */
    bool start() {
        if (isRunning) {
            NES_WARNING("Trying to start an already running source");
            return false;
        }

        NES_DEBUG("DataSource  {} : start source", Config::OperatorId);
        worker.emplace([this](const std::stop_token& stoken) {
            try {
                NES_DEBUG("DataSource {} call open", Config::OperatorId);
                if (!open()) {
                    NES_ERROR("Failed to open DataSource {}", Config::OperatorId);
                    executableSuccessors.stop(Runtime::QueryTerminationType::Failure);
                    return;
                }
                NES_DEBUG("DataSource {} start running routine", Config::OperatorId);
                runningRoutine(stoken);
                NES_DEBUG("DataSource {} call close", Config::OperatorId);
                if (!close(terminationType.load())) {
                    NES_WARNING("Could not close DataSource {}", Config::OperatorId);
                }
                NES_DEBUG("DataSource {} end running", Config::OperatorId);
                if (!stoken.stop_requested()) {
                    NES_INFO("Propagating Stop Request");
                    executableSuccessors.stop(Runtime::QueryTerminationType::Graceful);
                }
            } catch (const std::exception& exception) {
                NES_ERROR("Data Source Worker {} caught exception: {}", Config::OperatorId, exception.what());
            }
        });
        isRunning = true;
        return true;
    }

    /*P
     * @brief running routine while source is active
     */
    void runningRoutine(const std::stop_token& stoken) {
        static_assert(Config::OperatorId != 0, "The id of the source is not set properly");
        setThreadName(fmt::format("DataSrc-{}", Config::OperatorId).c_str());

        NES_DEBUG("DataSource {}: Running Data Source of type={} ",
                  Config::OperatorId,
                  magic_enum::enum_name(Impl::SourceType));

        if constexpr (!BufferLimitingConfig<Config>) {
            NES_DEBUG(
                "DataSource: the user does not specify the number of buffers to produce therefore we will produce buffer until "
                "the source is empty");
        } else {
            NES_DEBUG("DataSource: the user specify to produce {} buffers", Config::NumbersOfBufferToProduce);
        }

        size_t numberOfBuffersProduced = 0;
        while (!stoken.stop_requested()) {
            //check if already produced enough buffer
            if constexpr (BufferLimitingConfig<Config>) {
                if (Config::NumbersOfBufferToProduce >= numberOfBuffersProduced) {
                    NES_DEBUG("DataSource {}: Receiving thread terminated ... stopping because cnt={} smaller than "
                              "numBuffersToProcess={} now return",
                              Config::OperatorId,
                              numberOfBuffersProduced,
                              Config::NumbersOfBufferToProduce);
                    break;
                }
            }

            // TupleBuffer Lives in DataSource
            NES::Runtime::TupleBuffer tb;
            if constexpr (Impl::NeedsBuffer) {
                tb = allocateBuffer();
            }
            auto optBuf = impl.receiveData(stoken, BufferType::of(tb));
            if (stoken.stop_requested() && !optBuf.has_value()) {
                // necessary if source stops while receiveData is called due to stricter shutdown logic
                break;
            }
            //this checks we received a valid output buffer
            if (optBuf.has_value()) {
                tb = std::move(optBuf.value());
                generatedBuffers.fetch_add(1);
                generatedTuples.fetch_add(tb.getNumberOfTuples());
                emitWorkFromSource(tb);
                ++numberOfBuffersProduced;
            } else {
                NES_DEBUG("DataSource {}: stopping cause of invalid buffer", Config::OperatorId);
                NES_DEBUG("DataSource {}: Thread going to terminating with graceful exit.", Config::OperatorId);
                break;
            }
        }
    }

    /**
     * @brief debug function for testing to get number of generated tuples
     * @return number of generated tuples
     */
    [[nodiscard]] uint64_t getNumberOfGeneratedTuples() const { return generatedTuples.load(); }

    /**
     * @brief debug function for testing to get number of generated buffer
     * @return number of generated buffer
     */
    [[nodiscard]] uint64_t getNumberOfGeneratedBuffers() const { return generatedBuffers.load(); }

    /**
     * @brief Get number of buffers to be processed
     */
    [[nodiscard]] size_t getNumBuffersToProcess() const {
        if constexpr (BufferLimitingConfig<Config>) {
            return Config::NumbersOfBufferToProduce;
        } else {
            return 0;
        }
    }

    void stop(Runtime::QueryTerminationType type) {
        if (!isRunning) {
            NES_WARNING("Data Source {} was stopped, but it was not running", Config::OperatorId);
            return;
        }
        NES_ASSERT(worker->get_id() != std::this_thread::get_id(), "Worker tries to stop himself");

        terminationType.store(type);
        worker->request_stop();
        worker->join();
        isRunning = false;
    }

  private:
    /**
     * @brief This methods initializes thread-local state. For instance, it creates the local buffer pool and is necessary
     * because we cannot do it in the constructor.
     */
    bool open() {
        NES_INFO("Data source open");
        bufferManager = TheBufferManager->createFixedSizeBufferPool(Config::LocalBuffers);
        return impl.open();
    }

    /**
     * @brief This method cleans up thread-local state for the source.
     */
    bool close(Runtime::QueryTerminationType type) {
        bool implClose = impl.close(type);
        bufferManager->destroy();
        return implClose;
    }

    /**
     * @brief Emits a tuple buffer to the successors.
     * @param buffer
     */
    void emitWork(Runtime::TupleBuffer& buffer) { executableSuccessors.emit(buffer); }

    NES::Runtime::TupleBuffer allocateBuffer() { return bufferManager->getBufferBlocking(); }

    constexpr void emitWorkFromSource(Runtime::TupleBuffer& buffer) {
        {
            // Currently this assumes, that if a Source Implementation allocates its own buffers it is also responsible to
            // configure the buffer controlblock
            if constexpr (Impl::NeedsBuffer) {
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
            }
            NES_DEBUG("DataSource produced buffer {} type= {} tuples= {} "
                      "orgID={}",
                      Config::OperatorId,
                      magic_enum::enum_name(Config::SourceType::SourceType),
                      buffer.getNumberOfTuples(),
                      buffer.getOriginId());
            emitWork(buffer);
        }
    }

    Impl impl;
    NES::Unikernel::UnikernelPipelineExecutionContext executableSuccessors;

    Runtime::FixedSizeBufferPoolPtr bufferManager{nullptr};
    std::atomic<Runtime::QueryTerminationType> terminationType = Runtime::QueryTerminationType::Invalid;
    uint64_t maxSequenceNumber = 0;

    std::optional<std::jthread> worker;
    std::atomic<uint64_t> generatedTuples{0};
    std::atomic<uint64_t> generatedBuffers{0};
    bool isRunning = false;
};

template<typename Config, DataSourceImpl<Config> Impl>
using DataSourcePtr = std::shared_ptr<DataSource<Config, Impl>>;

}// namespace NES

#endif// NES_CORE_INCLUDE_SOURCES_DATASOURCE_HPP_
