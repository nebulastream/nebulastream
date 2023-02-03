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
#include <Network/NetworkSource.hpp>
#include <Runtime/Events.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Network {

NetworkSource::NetworkSource(SchemaPtr schema,
                             Runtime::BufferManagerPtr bufferManager,
                             Runtime::QueryManagerPtr queryManager,
                             NetworkManagerPtr networkManager,
                             NesPartition nesPartition,
                             NodeLocation sinkLocation,
                             size_t numSourceLocalBuffers,
                             std::chrono::milliseconds waitTime,
                             uint8_t retryTimes,
                             std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(std::move(schema),
                 std::move(bufferManager),
                 std::move(queryManager),
                 nesPartition.getOperatorId(),
                 /*default origin id for the network source this is always zero*/ 0,
                 numSourceLocalBuffers,
                 GatheringMode::INTERVAL_MODE,
                 std::move(successors)),
      networkManager(std::move(networkManager)), nesPartition(nesPartition), sinkLocation(std::move(sinkLocation)),
      waitTime(waitTime), retryTimes(retryTimes) {
    NES_ASSERT(this->networkManager, "Invalid network manager");
}

std::optional<Runtime::TupleBuffer> NetworkSource::receiveData() {
    NES_THROW_RUNTIME_ERROR("NetworkSource: ReceiveData() called, but method is invalid and should not be used.");
}

SourceType NetworkSource::getType() const { return NETWORK_SOURCE; }

NesPartition NetworkSource::getNesPartition() const {
    return nesPartition;
}

std::string NetworkSource::toString() const { return "NetworkSource: " + nesPartition.toString(); }

// this is necessary to use std::visit below (see example: https://en.cppreference.com/w/cpp/utility/variant/visit)
namespace detail {
template<class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
template<class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;
}// namespace detail

bool NetworkSource::bind() {
    auto emitter = shared_from_base<DataEmitter>();
    return networkManager->registerSubpartitionConsumer(nesPartition, sinkLocation, emitter);
}

bool NetworkSource::start() {
    using namespace Runtime;
    NES_DEBUG("NetworkSource: start called on " << nesPartition);
    auto emitter = shared_from_base<DataEmitter>();
    auto expected = false;
    if (running.compare_exchange_strong(expected, true)) {
        for (const auto& successor : executableSuccessors) {
            auto querySubPlanId = std::visit(detail::overloaded{[](DataSinkPtr sink) {
                                                                    return sink->getParentPlanId();
                                                                },
                                                                [](Execution::ExecutablePipelinePtr pipeline) {
                                                                    return pipeline->getQuerySubPlanId();
                                                                }},
                                             successor);
            auto queryId = std::visit(detail::overloaded{[](DataSinkPtr sink) {
                                                             return sink->getQueryId();
                                                         },
                                                         [](Execution::ExecutablePipelinePtr pipeline) {
                                                             return pipeline->getQueryId();
                                                         }},
                                      successor);

            auto newReconf = ReconfigurationMessage(queryId, querySubPlanId, Runtime::Initialize, shared_from_base<DataSource>());
            queryManager->addReconfigurationMessage(queryId, querySubPlanId, newReconf, true);
            break;// hack as currently we assume only one executableSuccessor
        }
        NES_DEBUG("NetworkSource: start completed on " << nesPartition);
        return true;
    }
    return false;
}

bool NetworkSource::stop(Runtime::QueryTerminationType type) {
    using namespace Runtime;
    bool expected = true;
    NES_ASSERT2_FMT(type == Runtime::QueryTerminationType::HardStop,
                    "NetworkSource::stop only supports HardStop :: partition " << nesPartition);
    if (running.compare_exchange_strong(expected, false)) {
        NES_DEBUG("NetworkSource: stop called on " << nesPartition << " sending hard eos");
        auto newReconf = ReconfigurationMessage(-1, -1, Runtime::HardEndOfStream, DataSource::shared_from_base<DataSource>());
        queryManager->addReconfigurationMessage(-1, -1, newReconf, false);
        queryManager->notifySourceCompletion(shared_from_base<DataSource>(), Runtime::QueryTerminationType::HardStop);
        for (const auto& successor : executableSuccessors) {
            std::visit(detail::overloaded{[this](DataSinkPtr sink) {
                                              auto queryId = sink->getQueryId();
                                              auto subPlanId = sink->getParentPlanId();
                                              auto newReconf =
                                                  ReconfigurationMessage(queryId, subPlanId, Runtime::HardEndOfStream, sink);
                                              queryManager->addReconfigurationMessage(queryId, subPlanId, newReconf, true);
                                          },
                                          [this](Execution::ExecutablePipelinePtr pipeline) {
                                              auto queryId = pipeline->getQueryId();
                                              auto subPlanId = pipeline->getQuerySubPlanId();
                                              auto newReconf =
                                                  ReconfigurationMessage(queryId, subPlanId, Runtime::HardEndOfStream, pipeline);
                                              queryManager->addReconfigurationMessage(queryId, subPlanId, newReconf, true);
                                          }},
                       successor);
        }
        NES_DEBUG("NetworkSource: stop called on " << nesPartition << " sent hard eos");
    } else {
        NES_DEBUG("NetworkSource: stop called on " << nesPartition << " but was already stopped");
    }
    return true;
}

void NetworkSource::onEvent(Runtime::BaseEvent&) {
    NES_DEBUG("NetworkSource: received an event");
//    if (event.getEventType() == Runtime::EventType::kCustomEvent) {
//        auto epochEvent = dynamic_cast<Runtime::CustomEventWrapper&>(event).data<Runtime::PropagateEpochEvent>();
//        auto epochBarrier = epochEvent->timestampValue();
//        auto queryId = epochEvent->queryIdValue();
//        auto success = queryManager->addEpochPropagation(shared_from_base<DataSource>(), queryId, epochBarrier);
//        if (success) {
//            NES_DEBUG("NetworkSource::onEvent: epoch" << epochBarrier << " queryId " << queryId << " propagated");
//        }
//        else {
//            NES_ERROR("NetworkSource::onEvent:: could not propagate epoch " << epochBarrier << " queryId " << queryId);
//        }
//    }
}

void NetworkSource::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& workerContext) {
    NES_DEBUG("NetworkSource: reconfigure() called " << nesPartition.toString());
    NES::DataSource::reconfigure(task, workerContext);
    bool isTermination = false;
    Runtime::QueryTerminationType terminationType;
    switch (task.getType()) {
        case Runtime::Initialize: {
            // we need to check again because between the invocations of
            // NetworkSource::start() and NetworkSource::reconfigure() the query might have
            // been stopped for some reasons
            if (networkManager->isPartitionConsumerRegistered(nesPartition) == PartitionRegistrationStatus::Deleted) {
                return;
            }
            auto channel = networkManager->registerSubpartitionEventProducer(sinkLocation,
                                                                             nesPartition,
                                                                             localBufferManager,
                                                                             waitTime,
                                                                             retryTimes);
            if (channel == nullptr) {
                NES_DEBUG("NetworkSource: reconfigure() cannot get event channel " << nesPartition.toString() << " on Thread "
                                                                                   << Runtime::NesThread::getId());
                return;// partition was deleted on the other side of the channel.. no point in waiting for a channel
            }
            workerContext.storeEventOnlyChannel(nesPartition.getOperatorId(), std::move(channel));
            NES_DEBUG("NetworkSource: reconfigure() stored event-channel " << nesPartition.toString() << " Thread "
                                                                           << Runtime::NesThread::getId());
            break;
        }
        case Runtime::Destroy: {
            // necessary as event channel are lazily created so in the case of an immediate stop
            // they might not be established yet
            terminationType = Runtime::QueryTerminationType::Graceful;
            isTermination = true;
            break;
        }
        case Runtime::HardEndOfStream: {
            terminationType = Runtime::QueryTerminationType::HardStop;
            isTermination = true;
        }
        case Runtime::SoftEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Graceful;
            isTermination = true;
            break;
        }
        case Runtime::PropagateEpoch: {
            auto* channel = workerContext.getEventOnlyNetworkChannel(nesPartition.getOperatorId());
            //on arrival of an epoch barrier trim data in buffer storages in network sinks that belong to one query plan
            auto epochMessage = task.getUserData<EpochMessage>();
            NES_DEBUG("Executing PropagateEpoch punctuation= " << epochMessage.getTimestamp());
            if (channel) {
                channel->sendEvent<Runtime::PropagateEpochEvent>(Runtime::EventType::kCustomEvent, epochMessage.getTimestamp(), epochMessage.getReplicationLevel());
            }
            break;
        }
        case Runtime::PropagateKEpoch: {
            auto* channel = workerContext.getEventOnlyNetworkChannel(nesPartition.getOperatorId());
            //on arrival of an epoch barrier trim data in buffer storages in network sinks that belong to one query plan
            auto epochMessage = task.getUserData<EpochMessage>();
            auto timestamp = epochMessage.getTimestamp();
            auto replicationLevel = epochMessage.getReplicationLevel();
            NES_DEBUG("Executing PropagateKEpoch punctuation= " << timestamp << " replication level= " << replicationLevel);
            if (channel) {
                channel->sendEvent<Runtime::PropagateKEpochEvent>(Runtime::EventType::kEpochPropagation, timestamp, replicationLevel);
            }
            break;
        }
        default: {
            break;
        }
    }
    if (isTermination) {
        workerContext.releaseEventOnlyChannel(nesPartition.getOperatorId(), terminationType);
        NES_DEBUG("NetworkSource: reconfigure() released channel on " << nesPartition.toString() << " Thread "
                                                                      << Runtime::NesThread::getId());
    }
}

void NetworkSource::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    NES_DEBUG("NetworkSource: postReconfigurationCallback() called " << nesPartition.toString());
    NES::DataSource::postReconfigurationCallback(task);
    switch (task.getType()) {
        case Runtime::FailEndOfStream: {
            NES_NOT_IMPLEMENTED();
        }
        case Runtime::Destroy:
        case Runtime::HardEndOfStream:
        case Runtime::SoftEndOfStream: {
            NES_DEBUG("NetworkSource: postReconfigurationCallback(): unregistering SubpartitionConsumer "
                      << nesPartition.toString());
            networkManager->unregisterSubpartitionConsumer(nesPartition);
            bool expected = true;
            if (running.compare_exchange_strong(expected, false)) {
                NES_DEBUG("NetworkSource is stopped on reconf task with id " << nesPartition.toString());
                queryManager->notifySourceCompletion(shared_from_base<DataSource>(),
                                                     task.getType() == Runtime::SoftEndOfStream
                                                         ? Runtime::QueryTerminationType::Graceful
                                                         : Runtime::QueryTerminationType::HardStop);
            }
            return;
        }
        default: {
            break;
        }
    }
}

void NetworkSource::runningRoutine(const Runtime::BufferManagerPtr&, const Runtime::QueryManagerPtr&) {
    NES_THROW_RUNTIME_ERROR("NetworkSource: runningRoutine() called, but method is invalid and should not be used.");
}
void NetworkSource::onEndOfStream(Runtime::QueryTerminationType terminationType) {
    // propagate EOS to the locally running QEPs that use the network source
    NES_DEBUG("Going to inject eos for " << nesPartition << " terminationType=" << terminationType);
    if (Runtime::QueryTerminationType::Graceful == terminationType) {
        queryManager->addEndOfStream(shared_from_base<DataSource>(), Runtime::QueryTerminationType::Graceful);
    } else {
        NES_WARNING("Ignoring forceful EoS on " << nesPartition);
    }
}

}// namespace NES::Network