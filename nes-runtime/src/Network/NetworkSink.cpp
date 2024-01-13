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
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Runtime/Events.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>

namespace NES::Network {

struct VersionUpdate {
    NodeLocation nodeLocation;
    NesPartition partition;
    QuerySubPlanVersion version;
};

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
                         uint64_t numberOfOrigins,
                         QuerySubPlanVersion version)
    : SinkMedium(
        std::make_shared<NesFormat>(schema, NES::Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getBufferManager()),
        nodeEngine,
        numOfProducers,
        queryId,
        querySubPlanId,
        numberOfOrigins),
      uniqueNetworkSinkDescriptorId(uniqueNetworkSinkDescriptorId), nodeEngine(nodeEngine),
      networkManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getNetworkManager()),
      queryManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getQueryManager()), receiverLocation(destination),
      bufferManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getBufferManager()), nesPartition(nesPartition),
      messageSequenceNumber(0), numOfProducers(numOfProducers), waitTime(waitTime), retryTimes(retryTimes), version(version) {
    NES_ASSERT(this->networkManager, "Invalid network manager");
    NES_DEBUG("NetworkSink: Created NetworkSink for partition {} location {}", nesPartition, destination.createZmqURI());
}

SinkMediumTypes NetworkSink::getSinkMediumType() { return SinkMediumTypes::NETWORK_SINK; }

bool NetworkSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
    NES_DEBUG("context {} writing data at sink {} on node {} for originId {} and seqNumber {}",
              workerContext.getId(),
              getUniqueNetworkSinkDescriptorId(),
              nodeEngine->getNodeId(),
              inputBuffer.getOriginId(),
              inputBuffer.getSequenceNumber());

    auto* channel = workerContext.getNetworkChannel(getUniqueNetworkSinkDescriptorId());

    //if async establishing of connection is in process, do not attempt to send data but buffer it instead
    //todo #4210: decrease amount of hashmap lookups
    if (!channel) {
        //todo #4311: check why sometimes buffers arrive after a channel has been closed
        NES_ASSERT2_FMT(workerContext.isAsyncConnectionInProgress(getUniqueNetworkSinkDescriptorId()),
                        "Trying to write to invalid channel while no connection is in progress");

        //check if connection was established and buffer it is has not yest been established
        if (!retrieveNewChannelAndUnbuffer(workerContext)) {
            NES_TRACE("context {} buffering data", workerContext.getId());
            workerContext.insertIntoReconnectBufferStorage(getUniqueNetworkSinkDescriptorId(), inputBuffer);
            return true;
        }

        //if a connection was established, retrieve the channel
        channel = workerContext.getNetworkChannel(getUniqueNetworkSinkDescriptorId());
    }

    //todo 4228: check if buffers are actually sent and not only inserted into to send queue
    return channel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes(), ++messageSequenceNumber);
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
            //check if the worker is configured to use async connecting
            if (nodeEngine->getConnectSinksAsync()) {
                //async connecting is activated. Delegate connection process to another thread and start the future
                auto reconf = Runtime::ReconfigurationMessage(queryId,
                                                              querySubPlanId,
                                                              Runtime::ReconfigurationType::ConnectionEstablished,
                                                              inherited0::shared_from_this(),
                                                              std::make_any<uint32_t>(numOfProducers));

                auto networkChannelFuture = networkManager->registerSubpartitionProducerAsync(receiverLocation,
                                                                                              nesPartition,
                                                                                              bufferManager,
                                                                                              waitTime,
                                                                                              retryTimes,
                                                                                              reconf,
                                                                                              queryManager,
                                                                                              version);
                workerContext.storeNetworkChannelFuture(getUniqueNetworkSinkDescriptorId(), std::move(networkChannelFuture));
                workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), nullptr);
            } else {
                //synchronous connecting is configured. let this thread wait on the connection being established
                auto channel = networkManager->registerSubpartitionProducer(receiverLocation,
                                                                            nesPartition,
                                                                            bufferManager,
                                                                            waitTime,
                                                                            retryTimes);
                NES_ASSERT(channel, "Channel not valid partition " << nesPartition);
                workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), std::move(channel));
                NES_DEBUG("NetworkSink: reconfigure() stored channel on {} Thread {} ref cnt {}",
                          nesPartition.toString(),
                          Runtime::NesThread::getId(),
                          task.getUserData<uint32_t>());
            }
            workerContext.setObjectRefCnt(this, task.getUserData<uint32_t>());
            workerContext.createStorage(nesPartition);
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
        case Runtime::ReconfigurationType::ConnectToNewReceiver: {
            //retrieve information about which source to connect to
            auto versionUpdate = task.getUserData<VersionUpdate>();
            auto newReceiverLocation = versionUpdate.nodeLocation;
            auto newPartition = versionUpdate.partition;
            auto newVersion = versionUpdate.version;
            if (newReceiverLocation == receiverLocation && newPartition == nesPartition) {
                NES_THROW_RUNTIME_ERROR("Attempting reconnect but the new source descriptor equals the old one");
            }

            if (workerContext.isAsyncConnectionInProgress(getUniqueNetworkSinkDescriptorId())) {
                workerContext.abortConnectionProcess(getUniqueNetworkSinkDescriptorId());
            }

            //todo: pass version here
            clearOldAndConnectToNewChannelAsync(workerContext, newReceiverLocation, newPartition, newVersion);
            break;
        }
        case Runtime::ReconfigurationType::ConnectionEstablished: {
            //if the callback was triggered by the channel for another thread becoming ready, we cannot do anything
            if (!workerContext.isAsyncConnectionInProgress(getUniqueNetworkSinkDescriptorId())) {
                NES_DEBUG("NetworkSink: reconfigure() No network channel future found for operator {} Thread {}",
                          nesPartition.toString(),
                          Runtime::NesThread::getId());
                break;
            }
            retrieveNewChannelAndUnbuffer(workerContext);
            break;
        }
        default: {
            break;
        }
    }
    if (terminationType != Runtime::QueryTerminationType::Invalid) {
        //todo #3013: make sure buffers are kept if the device is currently buffering
        if (workerContext.decreaseObjectRefCnt(this) == 1) {
            if (workerContext.isAsyncConnectionInProgress(getUniqueNetworkSinkDescriptorId())) {
                //wait until channel has either connected or connection times out
                NES_DEBUG("NetworkSink: reconfigure() waiting for channel to connect in order to unbuffer before shutdown. "
                          "operator {} Thread {}",
                          nesPartition.toString(),
                          Runtime::NesThread::getId());
                //todo #4309: make sure this function times out
                //the following call blocks until the channel has been established
                auto channel = workerContext.waitForAsyncConnection(getUniqueNetworkSinkDescriptorId());
                if (channel) {
                    NES_DEBUG("NetworkSink: reconfigure() established connection for operator {} Thread {}",
                              nesPartition.toString(),
                              Runtime::NesThread::getId());
                    workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), std::move(channel));
                } else {
                    networkManager->unregisterSubpartitionProducer(nesPartition);
                    //do not release network channel in the next step because none was established
                    return;
                }
            }
            unbuffer(workerContext);
            networkManager->unregisterSubpartitionProducer(nesPartition);
            NES_ASSERT2_FMT(workerContext.releaseNetworkChannel(getUniqueNetworkSinkDescriptorId(),
                                                                terminationType,
                                                                queryManager->getNumberOfWorkerThreads(),
                                                                messageSequenceNumber),
                            "Cannot remove network channel " << nesPartition.toString());
            /* store a nullptr in place of the released channel, in case another write happens afterwards, that will prevent crashing and
            allow throwing an error instead */
            workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), nullptr);
            NES_DEBUG("NetworkSink: reconfigure() released channel on {} Thread {}",
                      nesPartition.toString(),
                      Runtime::NesThread::getId());
        }
    }
}

void NetworkSink::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    NES_DEBUG("NetworkSink: postReconfigurationCallback() called {} parent plan {}", nesPartition.toString(), querySubPlanId);
    inherited0::postReconfigurationCallback(task);

    switch (task.getType()) {
        //update info about receiving network source to new target
        case Runtime::ReconfigurationType::ConnectToNewReceiver: {
            auto versionUpdate = task.getUserData<VersionUpdate>();
            networkManager->unregisterSubpartitionProducer(nesPartition);

            receiverLocation = versionUpdate.nodeLocation;
            nesPartition = versionUpdate.partition;
            version = versionUpdate.version;
            messageSequenceNumber = 0;

            break;
        }
        default: {
            break;
        }
    }
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

OperatorId NetworkSink::getUniqueNetworkSinkDescriptorId() const { return uniqueNetworkSinkDescriptorId; }

Runtime::NodeEnginePtr NetworkSink::getNodeEngine() { return nodeEngine; }

void NetworkSink::configureNewSinkDescriptor(const NetworkSinkDescriptor& newNetworkSinkDescriptor) {
    auto newReceiverLocation = newNetworkSinkDescriptor.getNodeLocation();
    auto newPartition = newNetworkSinkDescriptor.getNesPartition();
    auto newVersion = newNetworkSinkDescriptor.getVersion();
    VersionUpdate newReceiverTuple = {newReceiverLocation, newPartition, newVersion};
    //register event consumer for new source. It has to be registered before any data channels connect
    NES_ASSERT2_FMT(
        networkManager->registerSubpartitionEventConsumer(newReceiverLocation, newPartition, inherited1::shared_from_this()),
        "Cannot register event listener " << nesPartition.toString());
    Runtime::ReconfigurationMessage message = Runtime::ReconfigurationMessage(nesPartition.getQueryId(),
                                                                              querySubPlanId,
                                                                              Runtime::ReconfigurationType::ConnectToNewReceiver,
                                                                              inherited0::shared_from_this(),
                                                                              newReceiverTuple);
    queryManager->addReconfigurationMessage(nesPartition.getQueryId(), querySubPlanId, message, false);
}

void NetworkSink::clearOldAndConnectToNewChannelAsync(Runtime::WorkerContext& workerContext,
                                                      const NodeLocation& newNodeLocation,
                                                      NesPartition newNesPartition,
                                                      QuerySubPlanVersion newVersion) {
    NES_DEBUG("NetworkSink: method clearOldAndConnectToNewChannelAsync() called {} qep {}, by thread {}",
              nesPartition.toString(),
              querySubPlanId,
              Runtime::NesThread::getId());

    networkManager->unregisterSubpartitionProducer(nesPartition);

    auto reconf = Runtime::ReconfigurationMessage(queryId,
                                                  querySubPlanId,
                                                  Runtime::ReconfigurationType::ConnectionEstablished,
                                                  inherited0::shared_from_this(),
                                                  std::make_any<uint32_t>(numOfProducers));

    auto networkChannelFuture = networkManager->registerSubpartitionProducerAsync(newNodeLocation,
                                                                                  newNesPartition,
                                                                                  bufferManager,
                                                                                  waitTime,
                                                                                  retryTimes,
                                                                                  reconf,
                                                                                  queryManager,
                                                                                  newVersion);
    //todo: #4282 use QueryTerminationType::Redeployment
    workerContext.storeNetworkChannelFuture(getUniqueNetworkSinkDescriptorId(), std::move(networkChannelFuture));
    //todo #4310: do release and storing of nullptr in one call
    workerContext.releaseNetworkChannel(getUniqueNetworkSinkDescriptorId(),
                                        Runtime::QueryTerminationType::Graceful,
                                        queryManager->getNumberOfWorkerThreads(),
                                        messageSequenceNumber);
    workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), nullptr);
}

void NetworkSink::unbuffer(Runtime::WorkerContext& workerContext) {
    auto topBuffer = workerContext.removeBufferFromReconnectBufferStorage(getUniqueNetworkSinkDescriptorId());
    NES_INFO("sending buffered data");
    while (topBuffer) {
        if (!topBuffer.value().getBuffer()) {
            NES_WARNING("buffer does not exist");
            break;
        }
        if (!writeData(topBuffer.value(), workerContext)) {
            NES_WARNING("could not send all data from buffer");
            break;
        }
        NES_TRACE("buffer sent");
        topBuffer = workerContext.removeBufferFromReconnectBufferStorage(getUniqueNetworkSinkDescriptorId());
    }
}

bool NetworkSink::retrieveNewChannelAndUnbuffer(Runtime::WorkerContext& workerContext) {
    //retrieve new channel
    auto newNetworkChannelFutureOptional = workerContext.getAsyncConnectionResult(getUniqueNetworkSinkDescriptorId());

    //if the connection process did not finish yet, the reconfiguration was triggered by another thread.
    if (!newNetworkChannelFutureOptional.has_value()) {
        NES_DEBUG("NetworkSink: reconfigure() network channel has not connected yet for operator {} Thread {}",
                  nesPartition.toString(),
                  Runtime::NesThread::getId());
        return false;
    }

    //check if channel connected successfully
    if (newNetworkChannelFutureOptional.value() == nullptr) {
        NES_DEBUG("NetworkSink: reconfigure() network channel retrieved from future is null for operator {} Thread {}",
                  nesPartition.toString(),
                  Runtime::NesThread::getId());
        NES_ASSERT2_FMT(retryTimes != 0, "Received invalid channel although channel retry times are set to indefinite");

        //todo 4308: if there were finite retry attempts and they failed, do not crash the worker but fail the query
        NES_ASSERT2_FMT(false, "Could not establish network channel");
        return false;
    }
    workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), std::move(newNetworkChannelFutureOptional.value()));

    NES_INFO("stop buffering data for context {}", workerContext.getId());
    unbuffer(workerContext);
    return true;
}

bool NetworkSink::scheduleNewDescriptor(const NetworkSinkDescriptor& networkSinkDescriptor) {
    if (version != networkSinkDescriptor.getVersion()) {
        nextSinkDescriptor = networkSinkDescriptor;
        return true;
    }
    return false;
}

bool NetworkSink::applyNextSinkDescriptor() {
    if (!nextSinkDescriptor.has_value()) {
        return false;
    }
    configureNewSinkDescriptor(nextSinkDescriptor.value());
    return true;
}
}// namespace NES::Network
