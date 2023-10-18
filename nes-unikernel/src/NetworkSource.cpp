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

#include <Network/NesPartition.hpp>
#include <Network/NetworkChannel.hpp>
#include <Network/NetworkManager.hpp>
#include "NetworkSource.h"
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include "Runtime/Execution/UnikernelPipelineExecutionContext.h"
#include <utility>

namespace NES::Network {

    NetworkSource::NetworkSource(Runtime::BufferManagerPtr bufferManager,
                                 NES::Runtime::WorkerContext& workerContext,
                                 NetworkManagerPtr networkManager,
                                 NesPartition nesPartition,
                                 NodeLocation sinkLocation,
                                 size_t numSourceLocalBuffers,
                                 std::chrono::milliseconds waitTime,
                                 uint8_t retryTimes,
                                 NES::Unikernel::UnikernelPipelineExecutionContextBase* successors,
                                 const std::string &physicalSourceName)

            : DataSource(std::move(bufferManager),
                         workerContext,
                         nesPartition.getOperatorId(),
            /*default origin id for the network source this is always zero*/ 0,
                         numSourceLocalBuffers,
                         physicalSourceName,
                         std::move(successors)),
              networkManager(std::move(networkManager)), nesPartition(nesPartition),
              sinkLocation(std::move(sinkLocation)),
              waitTime(waitTime), retryTimes(retryTimes) {
        NES_ASSERT(this->networkManager, "Invalid network manager");
    }

    std::optional<Runtime::TupleBuffer> NetworkSource::receiveData() {
        NES_THROW_RUNTIME_ERROR("NetworkSource: ReceiveData() called, but method is invalid and should not be used.");
    }

    SourceType NetworkSource::getType() const { return SourceType::NETWORK_SOURCE; }

    std::string NetworkSource::toString() const { return "NetworkSource: " + nesPartition.toString(); }

// this is necessary to use std::visit below (see example: https://en.cppreference.com/w/cpp/utility/variant/visit)
    namespace detail {
        template<class... Ts>
        struct overloaded : Ts ... {
            using Ts::operator()...;
        };
        template<class... Ts>
        overloaded(Ts...) -> overloaded<Ts...>;
    }// namespace detail

    bool NetworkSource::bind() {
        return networkManager->registerSubpartitionConsumer(nesPartition, sinkLocation, this);
    }

    bool NetworkSource::start() {
        using namespace Runtime;
        NES_DEBUG("NetworkSource: start called on {}", nesPartition);
        auto expected = false;
        if (running.compare_exchange_strong(expected, true)) {
            NES_INFO("Network source {} started", nesPartition);
           return true;
        }
        return false;
    }

    bool NetworkSource::fail() {
        using namespace Runtime;
        bool expected = true;
        if (running.compare_exchange_strong(expected, false)) {
            NES_DEBUG("NetworkSource: fail called on {}", nesPartition);
            auto newReconf =
                    ReconfigurationMessage(-1, -1, ReconfigurationType::FailEndOfStream,
                                       nullptr);
            this->reconfigure(newReconf, workerContext);
            return true;
        }
        return false;
    }

    bool NetworkSource::stop(Runtime::QueryTerminationType type) {
        using namespace Runtime;
        bool expected = true;
        NES_ASSERT2_FMT(type == QueryTerminationType::HardStop,
                        "NetworkSource::stop only supports HardStop or Failure :: partition " << nesPartition);
        if (running.compare_exchange_strong(expected, false)) {
            NES_DEBUG("NetworkSource: stop called on {}", nesPartition);
            int invalidId = -1;
            auto newReconf = ReconfigurationMessage(invalidId,
                                                    invalidId,
                                                    ReconfigurationType::HardEndOfStream,
                                                    nullptr);
            NES_DEBUG("NetworkSource: stop called on {} sent hard eos", nesPartition);
            this->reconfigure(newReconf, workerContext);
        } else {
            NES_DEBUG("NetworkSource: stop called on {} but was already stopped", nesPartition);
        }
        return true;
    }

    void NetworkSource::onEvent(Runtime::BaseEvent &event) {
        NES_DEBUG("NetworkSource: received an event");
        if (event.getEventType() == Runtime::EventType::kCustomEvent) {
            auto epochEvent = dynamic_cast<Runtime::CustomEventWrapper &>(event).data<Runtime::PropagateEpochEvent>();
            auto epochBarrier = epochEvent->timestampValue();
            auto queryId = epochEvent->queryIdValue();
            auto success = true;
            if (success) {
                NES_DEBUG("NetworkSource::onEvent: epoch {} queryId {} propagated", epochBarrier, queryId);
            } else {
                NES_ERROR("NetworkSource::onEvent:: could not propagate epoch {} queryId {}", epochBarrier, queryId);
            }
        }
    }

    void NetworkSource::reconfigure(Runtime::ReconfigurationMessage &task, Runtime::WorkerContext &workerContext) {
        NES_DEBUG("NetworkSource: reconfigure() called {}", nesPartition.toString());
        bool isTermination = false;
        Runtime::QueryTerminationType terminationType;
        switch (task.getType()) {
            case Runtime::ReconfigurationType::Initialize: {
                // we need to check again because between the invocations of
                // NetworkSource::start() and NetworkSource::reconfigure() the query might have
                // been stopped for some reasons
                if (networkManager->isPartitionConsumerRegistered(nesPartition) ==
                    PartitionRegistrationStatus::Deleted) {
                    return;
                }

                auto channel = networkManager->registerSubpartitionEventProducer(sinkLocation,
                                                                                 nesPartition,
                                                                                 localBufferManager,
                                                                                 waitTime,
                                                                                 retryTimes);
                if (channel == nullptr) {
                    NES_DEBUG("NetworkSource: reconfigure() cannot get event channel {} on Thread {}",
                              nesPartition.toString(),
                              Runtime::NesThread::getId());
                    return;// partition was deleted on the other side of the channel.. no point in waiting for a channel
                }
                workerContext.storeEventOnlyChannel(nesPartition.getOperatorId(), std::move(channel));
                NES_DEBUG("NetworkSource: reconfigure() stored event-channel {} Thread {}",
                          nesPartition.toString(),
                          Runtime::NesThread::getId());
                break;
            }
            case Runtime::ReconfigurationType::Destroy: {
                // necessary as event channel are lazily created so in the case of an immediate stop
                // they might not be established yet
                terminationType = Runtime::QueryTerminationType::Graceful;
                isTermination = true;
                break;
            }
            case Runtime::ReconfigurationType::HardEndOfStream: {
                terminationType = Runtime::QueryTerminationType::HardStop;
                isTermination = true;
            }
            case Runtime::ReconfigurationType::SoftEndOfStream: {
                terminationType = Runtime::QueryTerminationType::Graceful;
                isTermination = true;
                break;
            }
            default: {
                break;
            }
        }
        if (isTermination) {
            workerContext.releaseEventOnlyChannel(nesPartition.getOperatorId(), terminationType);
            NES_DEBUG("NetworkSource: reconfigure() released channel on {} Thread {}",
                      nesPartition.toString(),
                      Runtime::NesThread::getId());
        }
    }

    void NetworkSource::runningRoutine(const Runtime::BufferManagerPtr &) {
        NES_THROW_RUNTIME_ERROR(
                "NetworkSource: runningRoutine() called, but method is invalid and should not be used.");
    }

    void NetworkSource::onEndOfStream(Runtime::QueryTerminationType terminationType) {
        // propagate EOS to the locally running QEPs that use the network source
        NES_DEBUG("Going to inject eos for {} terminationType={}", nesPartition, terminationType);
        if (Runtime::QueryTerminationType::Graceful == terminationType) {
        } else {
            NES_WARNING("Ignoring forceful EoS on {}", nesPartition);
        }
    }

    void NetworkSource::onEvent(Runtime::BaseEvent &event, Runtime::WorkerContextRef workerContext) {
        NES_DEBUG("NetworkSource::onEvent(event, wrkContext) called. operatorId: {}", this->operatorId);
        if (event.getEventType() == Runtime::EventType::kStartSourceEvent) {
            auto senderChannel = workerContext.getEventOnlyNetworkChannel(this->operatorId);
            senderChannel->sendEvent<Runtime::StartSourceEvent>();
        }
    }

}// namespace NES::Network
