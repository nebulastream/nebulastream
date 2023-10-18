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
#include <DataSource.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>// for Buf...
#include <Sinks/Mediums/SinkMedium.hpp>   // for Dat...
#include <Util/Logger/Logger.hpp>         // for NES...
#include <Util/ThreadNaming.hpp>          // for set...
#include <algorithm>                      // for find
#include <atomic>                         // for atomic
#include <bits/chrono.h>                  // for dur...
#include <exception>                      // for exc...
#include <future>                         // for pro...
#include <iostream>                       // for bas...
#include <limits>                         // for num...
#include <memory>                         // for sha...
#include <mutex>                          // for uni...
#include <optional>                       // for opt...
#include <pthread.h>                      // for pth...
#include <ratio>                          // for ratio
#include <sched.h>                        // for cpu...
#include <stddef.h>                       // for size_t
#include <stdint.h>                       // for uin...
#include <string>                         // for ope...
#include <string_view>                    // for bas...
#include <thread>                         // for thread
#include <utility>                        // for move
#include <variant>                        // for get
#include <vector>                         // for vector

#include "API/Schema.hpp"                                       // for Schema
#include "Common/Identifiers.hpp"                               // for Ope...
#include "Runtime/BufferManager.hpp"                            // for Buf...
#include "Runtime/Execution/DataEmitter.hpp"                    // for Dat...
#include "Runtime/Execution/ExecutablePipeline.hpp"        // for Tup...
#include "Runtime/QueryTerminationType.hpp"                     // for Que...
#include "Runtime/Reconfigurable.hpp"                           // for Rec...
#include "Runtime/RuntimeForwardRefs.hpp"                       // for Suc...
#include "Runtime/TupleBuffer.hpp"                              // for Tup...
#include "Runtime/WorkerContext.hpp"                            // for Wor...
#include "Util/VirtualEnableSharedFromThis.hpp"                 // for NES...
#include "Util/magicenum/magic_enum.hpp"                        // for enu...
#include "Windowing/Watermark/MultiOriginWatermarkProcessor.hpp"// for Ori...
#include "Windowing/WindowingForwardRefs.hpp"                   // for Sch...

namespace NES {
    namespace Runtime {
        class BaseEvent;
    }// namespace Runtime
}// namespace NES

using namespace std::string_literals;
namespace NES {


    DataSource::DataSource(Runtime::BufferManagerPtr bufferManager,
                           NES::Runtime::WorkerContext &workerContext,
                           OperatorId operatorId,
                           OriginId originId,
                           size_t numSourceLocalBuffers,
                           const std::string &physicalSourceName,
                           NES::Unikernel::UnikernelPipelineExecutionContextBase *executableSuccessors,
                           uint64_t sourceAffinity,
                           uint64_t taskQueueId)
            : DataEmitter(), workerContext(workerContext),
              localBufferManager(std::move(bufferManager)), executableSuccessors(std::move(executableSuccessors)),
              operatorId(operatorId),
              originId(originId), numSourceLocalBuffers(numSourceLocalBuffers), sourceAffinity(sourceAffinity),
              taskQueueId(taskQueueId),
              physicalSourceName(physicalSourceName) {
        NES_ASSERT(this->localBufferManager, "Invalid buffer manager");
        // TODO #4094: enable this exception -- currently many UTs are designed to assume empty executableSuccessors
        //    if (this->executableSuccessors.empty()) {
        //        throw Exceptions::RuntimeException("empty executable successors");
        //    }
    }

    void DataSource::emitWorkFromSource(Runtime::TupleBuffer &buffer) {
        // set the origin id for this source
        buffer.setOriginId(originId);
        // set the creation timestamp
        buffer.setCreationTimestampInMS(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::high_resolution_clock::now().time_since_epoch())
                        .count());
        // Set the sequence number of this buffer.
        // A data source generates a monotonic increasing sequence number
        maxSequenceNumber++;
        buffer.setSequenceNumber(maxSequenceNumber);
        emitWork(buffer);
    }

    namespace detail {
        template<class... Ts>
        struct overloaded : Ts ... {
            using Ts::operator()...;
        };
        template<class... Ts>
        overloaded(Ts...) -> overloaded<Ts...>;
    }// namespace detail
    void DataSource::emitWork(Runtime::TupleBuffer &buffer) {
        executableSuccessors->execute(buffer);
    }

    OperatorId DataSource::getOperatorId() const { return operatorId; }

    void DataSource::setOperatorId(OperatorId operatorId) { this->operatorId = operatorId; }

    DataSource::~DataSource() {
        NES_ASSERT(running == false, "Data source destroyed but thread still running... stop() was not called");
        NES_DEBUG("DataSource {}: Destroy Data Source.", operatorId);
    }

    bool DataSource::start() {
        NES_DEBUG("DataSource  {} : start source", operatorId);
        std::promise<bool> prom;
        std::unique_lock lock(startStopMutex);
        bool expected = false;
        std::shared_ptr<std::thread> thread{nullptr};
        if (!running.compare_exchange_strong(expected, true)) {
            NES_WARNING("DataSource {}: is already running", operatorId);
            return false;
        } else {
            type = getType();
            NES_DEBUG("DataSource {}: Spawn thread", operatorId);
            auto expected = false;
            if (wasStarted.compare_exchange_strong(expected, true)) {
                thread = std::make_shared<std::thread>([this, &prom]() {
                    // Create a cpu_set_t object representing a set of CPUs. Clear it and mark
                    // only CPU i as set.
#ifdef __linux__
                    if (sourceAffinity != std::numeric_limits<uint64_t>::max()) {
                        NES_ASSERT(sourceAffinity < std::thread::hardware_concurrency(),
                                   "pinning position is out of cpu range maxPosition=" << sourceAffinity);
                        cpu_set_t cpuset;
                        CPU_ZERO(&cpuset);
                        CPU_SET(sourceAffinity, &cpuset);
                        int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
                        if (rc != 0) {
                            NES_THROW_RUNTIME_ERROR(
                                    "Cannot set thread affinity on source thread " + std::to_string(operatorId));
                        }
                    } else {
                        NES_WARNING("Use default affinity for source");
                    }
#endif
                    prom.set_value(true);
                    runningRoutine();
                    NES_DEBUG("DataSource {}: runningRoutine is finished", operatorId);
                });
            }
        }
        if (thread) {
            thread->detach();
        }
        return prom.get_future().get();
    }

    bool DataSource::fail() {
        bool isStopped = stop(Runtime::QueryTerminationType::Failure);// this will block until the thread is stopped
        NES_DEBUG("Source {} stop executed= {}", operatorId, (isStopped ? "stopped" : "cannot stop"));
        {
            // it may happen that the source failed prior of sending its eos
            std::unique_lock lock(startStopMutex);// do not call stop if holding this mutex
            NES_DEBUG("Source {} has already injected failure? {}", operatorId,
                      (endOfStreamSent ? "EoS sent" : "cannot send EoS"));
            if (!this->endOfStreamSent) {
            }
            return isStopped && endOfStreamSent;
        }
        return false;
    }

    namespace detail {
        template<typename R, typename P>
        bool waitForFuture(std::future<bool> &&future, std::chrono::duration<R, P> &&deadline) {
            auto terminationStatus = future.wait_for(deadline);
            switch (terminationStatus) {
                case std::future_status::ready: {
                    return future.get();
                }
                default: {
                    return false;
                }
            }
        }
    }// namespace detail

    bool DataSource::stop(Runtime::QueryTerminationType graceful) {
        using namespace std::chrono_literals;
        // Do not call stop from the runningRoutine!
        {
            std::unique_lock lock(startStopMutex);// this mutex guards the thread variable
            wasGracefullyStopped = graceful;
        }

        refCounter++;
        if (refCounter != numberOfConsumerQueries) {
            return true;
        }

        NES_DEBUG("DataSource {}: Stop called and source is {}", operatorId, (running ? "running" : "not running"));
        bool expected = true;

        // TODO add wakeUp call if source is blocking on something, e.g., tcp socket
        // TODO in general this highlights how our source model has some issues

        // TODO this is also an issue in the current development of the StaticDataSource. If it is still running here, we give up to early and never join the thread.

        try {
            if (!running.compare_exchange_strong(expected, false)) {
                NES_DEBUG("DataSource {} was not running, retrieving future now...", operatorId);
                auto expected = false;
                if (wasStarted && futureRetrieved.compare_exchange_strong(expected, true)) {
                    NES_ASSERT2_FMT(detail::waitForFuture(completedPromise.get_future(), 60s),
                                    "Cannot complete future to stop source " << operatorId);
                }
                NES_DEBUG("DataSource {} was not running, future retrieved", operatorId);
                return true;// it's ok to return true because the source is stopped
            } else {
                NES_DEBUG("DataSource {} was running, retrieving future now...", operatorId);
                auto expected = false;
                NES_ASSERT2_FMT(wasStarted && futureRetrieved.compare_exchange_strong(expected, true)
                                && detail::waitForFuture(completedPromise.get_future(), 10min),
                                "Cannot complete future to stop source " << operatorId);
                NES_WARNING("Stopped Source {} = {}", operatorId, wasGracefullyStopped);
                return true;
            }
        } catch (...) {
            auto expPtr = std::current_exception();
            wasGracefullyStopped = Runtime::QueryTerminationType::Failure;
            try {
                if (expPtr) {
                    std::rethrow_exception(expPtr);
                }
            } catch (std::exception const &e) {// it would not work if you pass by value
                // i leave the following lines just as a reminder:
                // here we do not need to call notifySourceFailure because it is done from the main thread
                // the only reason to call notifySourceFailure is when the main thread was not stated
                if (!wasStarted) {
                }
                return true;
            }
        }

        return false;
    }

    void DataSource::open() { bufferManager = localBufferManager->createFixedSizeBufferPool(numSourceLocalBuffers); }

    void DataSource::close() {
        Runtime::QueryTerminationType queryTerminationType;
        {
            std::unique_lock lock(startStopMutex);
            queryTerminationType = this->wasGracefullyStopped;
        }
        if (queryTerminationType != Runtime::QueryTerminationType::Graceful) {
            // inject reconfiguration task containing end of stream
            std::unique_lock lock(startStopMutex);
            NES_ASSERT2_FMT(!endOfStreamSent, "Eos was already sent for source " << toString());
            NES_DEBUG("DataSource {} : Data Source add end of stream. Gracefully={}", operatorId, queryTerminationType);

            bufferManager->destroy();
        }
    }

    void DataSource::runningRoutine() {
        try {
            unikernelRunningRoutine();
            completedPromise.set_value(true);

        } catch (std::exception const &exception) {
            completedPromise.set_exception(std::make_exception_ptr(exception));
        } catch (...) {
            try {
                auto expPtr = std::current_exception();
                if (expPtr) {
                    completedPromise.set_exception(expPtr);
                    std::rethrow_exception(expPtr);
                }
            } catch (std::exception const &exception) {
            }
        }
        NES_DEBUG("DataSource {} end runningRoutine", operatorId);
    }

    void DataSource::unikernelRunningRoutine() {
        NES_ASSERT(this->operatorId != 0, "The id of the source is not set properly");
        std::string thName = "DataSrc-" + std::to_string(operatorId);
        setThreadName(thName.c_str());

        NES_DEBUG("DataSource {}: Running Data Source of type={} ", operatorId, magic_enum::enum_name(getType()));

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
                if (!running) {             // necessary if source stops while receiveData is called due to stricter shutdown logic
                    break;
                }
                //this checks we received a valid output buffer
                if (optBuf.has_value()) {
                    auto &buf = optBuf.value();
                    NES_TRACE(
                            "DataSource produced buffer {} type= {} string={}: Received Data: {} tuples iteration= {} "
                            "operatorId={} orgID={}",
                            operatorId,
                            magic_enum::enum_name(getType()),
                            toString(),
                            buf.getNumberOfTuples(),
                            numberOfBuffersProduced,
                            this->operatorId,
                            this->operatorId);

                    emitWorkFromSource(buf);
                    ++numberOfBuffersProduced;
                } else {
                    NES_DEBUG("DataSource {}: stopping cause of invalid buffer", operatorId);
                    running = false;
                    NES_DEBUG("DataSource {}: Thread going to terminating with graceful exit.", operatorId);
                }
            } else {
                NES_DEBUG("DataSource {}: Receiving thread terminated ... stopping because cnt={} smaller than "
                          "numBuffersToProcess={} now return",
                          operatorId,
                          numberOfBuffersProduced,
                          numberOfBuffersToProduce);
                running = false;
            }
            NES_TRACE("DataSource {} : Data Source finished processing iteration {}", operatorId,
                      numberOfBuffersProduced);
        }
        NES_DEBUG("DataSource {} call close", operatorId);
        close();
        NES_DEBUG("DataSource {} end running", operatorId);
    }

    bool DataSource::injectEpochBarrier(uint64_t epochBarrier, uint64_t queryId) {
        NES_DEBUG("DataSource::injectEpochBarrier received timestamp  {} with queryId  {}", epochBarrier, queryId);
        return true;
    }

// debugging
    uint64_t DataSource::getNumberOfGeneratedTuples() const { return generatedTuples; };

    uint64_t DataSource::getNumberOfGeneratedBuffers() const { return generatedBuffers; };

    uint64_t DataSource::getNumBuffersToProcess() const { return numberOfBuffersToProduce; }

    std::vector<Schema::MemoryLayoutType> DataSource::getSupportedLayouts() {
        return {Schema::MemoryLayoutType::ROW_LAYOUT};
    }

    bool DataSource::checkSupportedLayoutTypes(SchemaPtr &schema) {
        auto supportedLayouts = getSupportedLayouts();
        return std::find(supportedLayouts.begin(), supportedLayouts.end(), schema->getLayoutType()) !=
               supportedLayouts.end();
    }

    void DataSource::onEvent(Runtime::BaseEvent &event) {
        NES_DEBUG("DataSource::onEvent(event) called. operatorId: {}", this->operatorId);
        // no behaviour needed, call onEvent of direct ancestor
        DataEmitter::onEvent(event);
    }

    void DataSource::onEvent(Runtime::BaseEvent &event, Runtime::WorkerContextRef) {
        NES_DEBUG("DataSource::onEvent(event, wrkContext) called. operatorId:  {}", this->operatorId);
        onEvent(event);
    }

}// namespace NES
