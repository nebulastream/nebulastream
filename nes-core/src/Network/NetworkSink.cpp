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
#include <Network/NetworkChannel.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Runtime/Events.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES::Network {

NetworkSink::NetworkSink(const SchemaPtr& schema,
                         uint64_t uniqueNetworkSinkDescriptorId,
                         QueryId queryId,
                         QuerySubPlanId querySubPlanId,
                         const NodeLocation& destination,
                         NesPartition nesPartition,
                         Runtime::NodeEnginePtr nodeEngine,
                         size_t numOfProducers,
                         std::chrono::milliseconds waitTime,
                         uint8_t retryTimes,
                         FaultToleranceType::Value faultToleranceType,
                         uint64_t numberOfOrigins)
    : inherited0(std::make_shared<NesFormat>(schema, Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getBufferManager()),
                 nodeEngine,
                 numOfProducers,
                 queryId,
                 querySubPlanId,
                 faultToleranceType,
                 numberOfOrigins),
      uniqueNetworkSinkDescriptorId(uniqueNetworkSinkDescriptorId), nodeEngine(nodeEngine),
      networkManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getNetworkManager()),
      queryManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getQueryManager()), receiverLocation(destination),
      bufferManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getBufferManager()), nesPartition(nesPartition),
      numOfProducers(numOfProducers), waitTime(waitTime), retryTimes(retryTimes), reconnectBuffering(false) {
    NES_ASSERT(this->networkManager, "Invalid network manager");
    NES_DEBUG("NetworkSink: Created NetworkSink for partition " << nesPartition << " location " << destination.createZmqURI());
    if (faultToleranceType == FaultToleranceType::AT_LEAST_ONCE || faultToleranceType == FaultToleranceType::AT_MOST_ONCE) {
        insertIntoStorageCallback = [this](Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
            workerContext.insertIntoStorage(this->nesPartition, inputBuffer);
        };
    } else {
        insertIntoStorageCallback = [](Runtime::TupleBuffer&, Runtime::WorkerContext&) {
        };
    }
    if (faultToleranceType == FaultToleranceType::AT_MOST_ONCE) {
        sendPropagationCallback = [this](Runtime::TupleBuffer& inputBuffer) {
            if (!(bufferCount % buffersPerEpoch) && bufferCount != 0) {
                auto epochBarrier = inputBuffer.getWatermark();
                auto success = queryManager->propagateEpochBackwards(this->querySubPlanId, epochBarrier);
                if (success) {
                    NES_DEBUG("NetworkSink::writeData: epoch" << epochBarrier << " queryId " << this->queryId << " sent back");
                } else {
                    NES_ERROR("NetworkSink::writeData:: couldn't send " << epochBarrier << " queryId " << this->queryId);
                }
            }
        };
    }
    else {
        sendPropagationCallback = [](Runtime::TupleBuffer&) {};
    }
    bufferCount = 0;
}

SinkMediumTypes NetworkSink::getSinkMediumType() { return NETWORK_SINK; }

bool NetworkSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
    //if a mobile node is in the process of reconnecting, do not attempt to send data but buffer it instead
    NES_TRACE("context " << workerContext.getId() << " writing data");
    if (reconnectBuffering) {
        NES_TRACE("context " << workerContext.getId() << " buffering data");
        workerContext.insertIntoStorage(this->nesPartition, inputBuffer);
        return true;
    }

    auto* channel = workerContext.getNetworkChannel(nesPartition.getOperatorId());
    if (channel) {
        insertIntoStorageCallback(inputBuffer, workerContext);
        auto success = channel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes());
        sendPropagationCallback(inputBuffer);
        bufferCount++;
        return success;
    }
    NES_ASSERT2_FMT(false, "invalid channel on " << nesPartition);
    return false;
}

void NetworkSink::preSetup() {
    NES_DEBUG("NetworkSink: method preSetup() called " << nesPartition.toString() << " qep " << querySubPlanId);
    NES_ASSERT2_FMT(
        networkManager->registerSubpartitionEventConsumer(receiverLocation, nesPartition, inherited1::shared_from_this()),
        "Cannot register event listener " << nesPartition.toString());
}

void NetworkSink::setup() {
    NES_DEBUG("NetworkSink: method setup() called " << nesPartition.toString() << " qep " << querySubPlanId);
    auto reconf = Runtime::ReconfigurationMessage(queryId,
                                                  querySubPlanId,
                                                  Runtime::Initialize,
                                                  inherited0::shared_from_this(),
                                                  std::make_any<uint32_t>(numOfProducers));
    queryManager->addReconfigurationMessage(queryId, querySubPlanId, reconf, true);
}

void NetworkSink::shutdown() {
    NES_DEBUG("NetworkSink: shutdown() called " << nesPartition.toString() << " queryId " << queryId << " qepsubplan "
                                                << querySubPlanId);
    networkManager->unregisterSubpartitionProducer(nesPartition);
}

std::string NetworkSink::toString() const { return "NetworkSink: " + nesPartition.toString(); }

void NetworkSink::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& workerContext) {
    NES_DEBUG("NetworkSink: reconfigure() called " << nesPartition.toString() << " parent plan " << querySubPlanId);
    inherited0::reconfigure(task, workerContext);
    Runtime::QueryTerminationType terminationType = Runtime::QueryTerminationType::Invalid;
    switch (task.getType()) {
        case Runtime::Initialize: {
            auto channel = networkManager->registerSubpartitionProducer(receiverLocation,
                                                                        nesPartition,
                                                                        bufferManager,
                                                                        waitTime,
                                                                        retryTimes);
            NES_ASSERT(channel, "Channel not valid partition " << nesPartition);
            workerContext.storeNetworkChannel(nesPartition.getOperatorId(), std::move(channel));
            workerContext.setObjectRefCnt(this, task.getUserData<uint32_t>());
            workerContext.createStorage(nesPartition);
            NES_DEBUG("NetworkSink: reconfigure() stored channel on " << nesPartition.toString() << " Thread "
                                                                      << Runtime::NesThread::getId() << " ref cnt "
                                                                      << task.getUserData<uint32_t>());

            break;
        }
        case Runtime::HardEndOfStream: {
            terminationType = Runtime::QueryTerminationType::HardStop;
            break;
        }
        case Runtime::SoftEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Graceful;
            break;
        }
        case Runtime::FailEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Failure;
            break;
        }
        case Runtime::PropagateEpoch: {
//            auto* channel = workerContext.getNetworkChannel(nesPartition.getOperatorId());
            //on arrival of an epoch barrier trim data in buffer storages in network sinks that belong to one query plan
            auto timestamp = task.getUserData<uint64_t>();
//            NES_DEBUG("Executing PropagateEpoch on qep queryId=" << queryId
                                                                 << "punctuation= " << timestamp);
//            channel->sendEvent<Runtime::PropagateEpochEvent>(Runtime::EventType::kCustomEvent, timestamp, queryId);
            workerContext.trimStorage(nesPartition, timestamp);
            break;
        }
        case Runtime::StartBuffering: {
            //reconnect buffering is currently not supported if tuples are also buffered for fault tolerance
            //todo #3014: make reconnect buffering and fault tolerance buffering compatible
            if (faultToleranceType == FaultToleranceType::AT_LEAST_ONCE
                || faultToleranceType == FaultToleranceType::EXACTLY_ONCE) {
                break;
            }
            if (reconnectBuffering) {
                NES_DEBUG("Requested sink to buffer but it is already buffering")
            } else {
                this->reconnectBuffering = true;
            }
            break;
        }
        case Runtime::StopBuffering: {
            //reconnect buffering is currently not supported if tuples are also buffered for fault tolerance
            //todo #3014: make reconnect buffering and fault tolerance buffering compatible
            if (faultToleranceType == FaultToleranceType::AT_LEAST_ONCE
                || faultToleranceType == FaultToleranceType::EXACTLY_ONCE) {
                break;
            }
            /*stop buffering new incoming tuples. this will change the order of the tuples if new tuples arrive while we
            unbuffer*/
            reconnectBuffering = false;
            NES_INFO("stop buffering data for context " << workerContext.getId());
            auto topBuffer = workerContext.getTopTupleFromStorage(nesPartition);
            NES_INFO("sending buffered data")
            while (topBuffer) {
                /*this will only work if guarantees are not set to at least once,
                otherwise new tuples could be written to the buffer at the same time causing conflicting writes*/
                if (!topBuffer.value().getBuffer()) {
                    NES_WARNING("buffer does not exist");
                    break;
                }
                if (!writeData(topBuffer.value(), workerContext)) {
                    NES_WARNING("could not send all data from buffer")
                    break;
                }
                NES_TRACE("buffer sent")
                workerContext.removeTopTupleFromStorage(nesPartition);
                topBuffer = workerContext.getTopTupleFromStorage(nesPartition);
            }
            break;
        }
        default: {
            break;
        }
    }
    if (terminationType != Runtime::QueryTerminationType::Invalid) {
        //todo #3013: make sure buffers are kept if the device is currently buffering
        if (workerContext.decreaseObjectRefCnt(this) == 1) {
            networkManager->unregisterSubpartitionProducer(nesPartition);
            NES_ASSERT2_FMT(workerContext.releaseNetworkChannel(nesPartition.getOperatorId(), terminationType),
                            "Cannot remove network channel " << nesPartition.toString());
            NES_DEBUG("NetworkSink: reconfigure() released channel on " << nesPartition.toString() << " Thread "
                                                                        << Runtime::NesThread::getId());
        }
    }
}

void NetworkSink::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    NES_DEBUG("NetworkSink: postReconfigurationCallback() called " << nesPartition.toString() << " parent plan "
                                                                   << querySubPlanId);
    inherited0::postReconfigurationCallback(task);
}

void NetworkSink::onEvent(Runtime::BaseEvent& event) {
    NES_DEBUG("NetworkSink::onEvent(event) called. uniqueNetworkSinkDescriptorId: " << this->uniqueNetworkSinkDescriptorId);
    auto qep = queryManager->getQueryExecutionPlan(querySubPlanId);
    qep->onEvent(event);

    NES_DEBUG("NetworkSink: received an event");
    if (event.getEventType() == Runtime::EventType::kCustomEvent) {
        auto epochEvent = dynamic_cast<Runtime::CustomEventWrapper&>(event).data<Runtime::PropagateEpochEvent>();
        if (epochEvent) {
            auto epochBarrier = epochEvent->timestampValue();
            auto success = queryManager->sendTrimmingReconfiguration(querySubPlanId, epochBarrier);
            if (success) {
                NES_DEBUG("NetworkSink::onEvent: epoch" << epochBarrier << " queryId " << queryId << " trimmed");
                success = queryManager->propagateEpochBackwards(querySubPlanId, epochBarrier);
                if (success) {
                    NES_DEBUG("NetworkSink::onEvent: epoch" << epochBarrier << " queryId " << queryId << " sent further");
                }
                else {
                    NES_INFO("NetworkSink::onEvent:: end of propagation " << epochBarrier << " queryId " << queryId);
                }
            }
            else {
                NES_ERROR("NetworkSink::onEvent:: could not trim " << epochBarrier << " queryId " << queryId);
            }
        }
        else {
            auto epochEvent = dynamic_cast<Runtime::CustomEventWrapper&>(event).data<Runtime::PropagateKEpochEvent>();
            auto epochBarrier = epochEvent->timestampValue();
            auto replicationLevel = epochEvent->replicationLevelValue();
            if (replicationLevel > 0) {
                auto success = queryManager->propagateKEpochBackwards(querySubPlanId, epochBarrier, replicationLevel - 1);
                if (success) {
                    NES_DEBUG("NetworkSink::onEvent: epoch" << epochBarrier << " queryId " << queryId << " sent further");
                }
                else {
                    NES_INFO("NetworkSink::onEvent:: end of propagation " << epochBarrier << " queryId " << queryId);
                }
            }
            else {
                auto success = queryManager->sendTrimmingReconfiguration(querySubPlanId, epochBarrier);
                if (success) {
                    NES_DEBUG("NetworkSink::onEvent: epoch" << epochBarrier << " queryId " << queryId << " trimmed");
                    success = queryManager->propagateKEpochBackwards(querySubPlanId, epochBarrier, 0);
                    if (success) {
                        NES_DEBUG("NetworkSink::onEvent: epoch" << epochBarrier << " queryId " << queryId << " sent further");
                    }
                    else {
                        NES_INFO("NetworkSink::onEvent:: end of propagation " << epochBarrier << " queryId " << queryId);
                    }
                }
                else {
                    NES_ERROR("NetworkSink::onEvent:: could not trim " << epochBarrier << " queryId " << queryId);
                }
            }
        }
    }
    else if (event.getEventType() == Runtime::EventType::kStartSourceEvent) {
        // todo jm continue here. how to obtain local worker context?
    }
}

void NetworkSink::onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef) {
    NES_DEBUG(
        "NetworkSink::onEvent(event, wrkContext) called. uniqueNetworkSinkDescriptorId: " << this->uniqueNetworkSinkDescriptorId);
    // this function currently has no usage
    onEvent(event);
}

OperatorId NetworkSink::getUniqueNetworkSinkDescriptorId() { return uniqueNetworkSinkDescriptorId; }

Runtime::NodeEnginePtr NetworkSink::getNodeEngine() { return nodeEngine; }

}// namespace NES::Network
