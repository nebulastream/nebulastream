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
#include <Util/Common.hpp>
#include <Util/Core.hpp>

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
                         FaultToleranceType faultToleranceType,
                         uint64_t numberOfOrigins,
                         bool connectAsync)
    : SinkMedium(
        std::make_shared<NesFormat>(schema, NES::Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getBufferManager()),
        nodeEngine,
        numOfProducers,
        queryId,
        querySubPlanId,
        faultToleranceType,
        numberOfOrigins,
        nullptr),
      uniqueNetworkSinkDescriptorId(uniqueNetworkSinkDescriptorId), nodeEngine(nodeEngine),
      networkManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getNetworkManager()),
      queryManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getQueryManager()), receiverLocation(destination),
      bufferManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getBufferManager()), nesPartition(nesPartition),
      numOfProducers(numOfProducers), waitTime(waitTime), retryTimes(retryTimes), reconnectBuffering(false), connectAsync(connectAsync) {
    NES_ASSERT(this->networkManager, "Invalid network manager");
    NES_DEBUG("NetworkSink: Created NetworkSink for partition {} location {}", nesPartition, destination.createZmqURI());
    if (faultToleranceType == FaultToleranceType::AT_LEAST_ONCE) {
        insertIntoStorageCallback = [this](Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
            workerContext.insertIntoStorage(this->nesPartition, inputBuffer);
        };
    } else {
        insertIntoStorageCallback = [](Runtime::TupleBuffer&, Runtime::WorkerContext&) {
        };
    }
}

SinkMediumTypes NetworkSink::getSinkMediumType() { return SinkMediumTypes::NETWORK_SINK; }

bool NetworkSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
    //if a mobile node is in the process of reconnecting, do not attempt to send data but buffer it instead
    NES_TRACE("context {} writing data", workerContext.getId());
    //if (reconnectBuffering) {
    if (workerContext.checkNetwokChannelFutureExistence(nesPartition.getOperatorId())) {
        NES_TRACE("context {} buffering data", workerContext.getId());
        workerContext.insertIntoStorage(this->nesPartition, inputBuffer);
        return true;
    }

    auto* channel = workerContext.getNetworkChannel(nesPartition.getOperatorId());
    if (channel) {
        auto success = channel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes());
        if (success) {
            insertIntoStorageCallback(inputBuffer, workerContext);
        } else {
            //todo: initiate reconnect
        }
        return success;
    }
    NES_ASSERT2_FMT(false, "invalid channel on " << nesPartition);
    return false;
}

void NetworkSink::preSetup() {
    NES_DEBUG("NetworkSink: method preSetup() called {} qep {}", nesPartition.toString(), querySubPlanId);
    NES_ASSERT2_FMT(
        networkManager->registerSubpartitionEventConsumer(receiverLocation, nesPartition, inherited1::shared_from_this()),
        "Cannot register event listener " << nesPartition.toString());
}

void NetworkSink::setup() {
    NES_DEBUG("NetworkSink: method setup() called {} qep {}", nesPartition.toString(), querySubPlanId);
    auto reconf = Runtime::ReconfigurationMessage(queryId,
                                                  querySubPlanId,
                                                  Runtime::ReconfigurationType::Initialize,
                                                  inherited0::shared_from_this(),
                                                  std::make_any<uint32_t>(numOfProducers));
    queryManager->addReconfigurationMessage(queryId, querySubPlanId, reconf, true);
}

void NetworkSink::shutdown() {
    NES_DEBUG("NetworkSink: shutdown() called {} queryId {} qepsubplan {}", nesPartition.toString(), queryId, querySubPlanId);
    networkManager->unregisterSubpartitionProducer(nesPartition);
}

std::string NetworkSink::toString() const { return "NetworkSink: " + nesPartition.toString(); }

void NetworkSink::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& workerContext) {
    NES_DEBUG("NetworkSink: reconfigure() called {} qep {}", nesPartition.toString(), querySubPlanId);
    inherited0::reconfigure(task, workerContext);
    Runtime::QueryTerminationType terminationType = Runtime::QueryTerminationType::Invalid;
    switch (task.getType()) {
        case Runtime::ReconfigurationType::Initialize: {
            if (connectAsync) {
                connectToChannelAsync(workerContext);
            } else {
                auto channel =
                    networkManager->registerSubpartitionProducer(receiverLocation, nesPartition, bufferManager, waitTime, retryTimes);
                NES_ASSERT(channel, "Channel not valid partition " << nesPartition);
                workerContext.storeNetworkChannel(nesPartition.getOperatorId(), std::move(channel));
            }
            workerContext.setObjectRefCnt(this, task.getUserData<uint32_t>());
            workerContext.createStorage(nesPartition);
            NES_DEBUG("NetworkSink: reconfigure() stored channel on {} Thread {} ref cnt {}",
                      nesPartition.toString(),
                      Runtime::NesThread::getId(),
                      task.getUserData<uint32_t>());
            break;
        }
        case Runtime::ReconfigurationType::HardEndOfStream: {
            terminationType = Runtime::QueryTerminationType::HardStop;
            break;
        }
        case Runtime::ReconfigurationType::SoftEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Graceful;
            break;
        }
        case Runtime::ReconfigurationType::FailEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Failure;
            break;
        }
        case Runtime::ReconfigurationType::PropagateEpoch: {
            auto* channel = workerContext.getNetworkChannel(nesPartition.getOperatorId());
            //on arrival of an epoch barrier trim data in buffer storages in network sinks that belong to one query plan
            auto timestamp = task.getUserData<uint64_t>();
            NES_DEBUG("Executing PropagateEpoch on qep queryId={} punctuation={}", queryId, timestamp);
            channel->sendEvent<Runtime::PropagateEpochEvent>(Runtime::EventType::kCustomEvent, timestamp, queryId);
            workerContext.trimStorage(nesPartition, timestamp);
            break;
        }
        case Runtime::ReconfigurationType::StartBuffering: {
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
        case Runtime::ReconfigurationType::StopBuffering: {
            //reconnect buffering is currently not supported if tuples are also buffered for fault tolerance
            //todo #3014: make reconnect buffering and fault tolerance buffering compatible
            if (faultToleranceType == FaultToleranceType::AT_LEAST_ONCE
                || faultToleranceType == FaultToleranceType::EXACTLY_ONCE) {
                break;
            }
            /*stop buffering new incoming tuples. this will change the order of the tuples if new tuples arrive while we
            unbuffer*/
            //reconnectBuffering = false;

            //if the callback was triggered by the channel for another thread becoming ready, we cannot to anything
            if (!workerContext.checkNetwokChannelFutureExistence(nesPartition.getOperatorId())) {
                return;
            }

            //retrieve new channel and replace old channel
            auto newNetworkChannel = workerContext.getNetworkChannelFuture(nesPartition.getOperatorId());
            workerContext.storeNetworkChannel(nesPartition.getOperatorId(), std::move(newNetworkChannel));

            //todo: extract to unbuffer function
            NES_INFO("stop buffering data for context {}", workerContext.getId());
            unbuffer(workerContext);
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
            if (workerContext.checkNetwokChannelFutureExistence(nesPartition.getOperatorId())) {
                //wait until channel has either connected or connection times out, so we do not an channel open

                auto channel = workerContext.waitForNetworkChannelFuture(nesPartition.getOperatorId());
                if (channel) {
                    workerContext.storeNetworkChannel(nesPartition.getOperatorId(), std::move(channel));
                    unbuffer(workerContext);
                    //channel->close(terminationType);
                } else {
                    //do not release network channel in the next step because none was established
                    return;
                }

                //todo: adjust messages
                NES_DEBUG("NetworkSink: reconfigure() released channel future on {} Thread {}",
                          nesPartition.toString(),
                          Runtime::NesThread::getId());

            }
            NES_ASSERT2_FMT(workerContext.releaseNetworkChannel(nesPartition.getOperatorId(), terminationType),
                            "Cannot remove network channel " << nesPartition.toString());
            NES_DEBUG("NetworkSink: reconfigure() released channel on {} Thread {}",
                      nesPartition.toString(),
                      Runtime::NesThread::getId());
        }
    }
}

void NetworkSink::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    NES_DEBUG("NetworkSink: postReconfigurationCallback() called {} parent plan {}", nesPartition.toString(), querySubPlanId);
    inherited0::postReconfigurationCallback(task);
}

void NetworkSink::onEvent(Runtime::BaseEvent& event) {
    NES_DEBUG("NetworkSink::onEvent(event) called. uniqueNetworkSinkDescriptorId: {}", this->uniqueNetworkSinkDescriptorId);
    auto qep = queryManager->getQueryExecutionPlan(querySubPlanId);
    qep->onEvent(event);

    if (event.getEventType() == Runtime::EventType::kStartSourceEvent) {
        // todo jm continue here. how to obtain local worker context?
    }
}
void NetworkSink::onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef) {
    NES_DEBUG("NetworkSink::onEvent(event, wrkContext) called. uniqueNetworkSinkDescriptorId: {}",
              this->uniqueNetworkSinkDescriptorId);
    // this function currently has no usage
    onEvent(event);
}

OperatorId NetworkSink::getUniqueNetworkSinkDescriptorId() { return uniqueNetworkSinkDescriptorId; }

Runtime::NodeEnginePtr NetworkSink::getNodeEngine() { return nodeEngine; }

void NetworkSink::connectToChannelAsync(Runtime::WorkerContext& workerContext) {
    NES_DEBUG("NetworkSink: method connectToChannelAsync() called {} qep {}", nesPartition.toString(), querySubPlanId);
    auto reconf = Runtime::ReconfigurationMessage(queryId,
                                                  querySubPlanId,
                                                  Runtime::ReconfigurationType::StopBuffering,
                                                  inherited0::shared_from_this(),
                                                  std::make_any<uint32_t>(numOfProducers));
    auto networkChannelFuture = networkManager->registerSubpartitionProducerAsync(receiverLocation,
                                                                             nesPartition,
                                                                             bufferManager,
                                                                             waitTime,
                                                                             retryTimes, reconf, queryManager);
    workerContext.storeNetworkChannelFuture(nesPartition.getOperatorId(), std::move(networkChannelFuture));
    //reconnectBuffering = true;
}

void NetworkSink::unbuffer(Runtime::WorkerContext& workerContext) {
    auto topBuffer = workerContext.getTopTupleFromStorage(nesPartition);
    NES_INFO("sending buffered data");
    while (topBuffer) {
        /*this will only work if guarantees are not set to at least once,
                otherwise new tuples could be written to the buffer at the same time causing conflicting writes*/
        if (!topBuffer.value().getBuffer()) {
            NES_WARNING("buffer does not exist");
            break;
        }
        if (!writeData(topBuffer.value(), workerContext)) {
            NES_WARNING("could not send all data from buffer");
            break;
        }
        NES_TRACE("buffer sent");
        workerContext.removeTopTupleFromStorage(nesPartition);
        topBuffer = workerContext.getTopTupleFromStorage(nesPartition);
    }
}



}// namespace NES::Network
