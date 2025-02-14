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
#include <Reconfiguration/Metadata/DrainQueryMetadata.hpp>
#include <Reconfiguration/Metadata/UpdateAndDrainQueryMetadata.hpp>
#include <Runtime/Events.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Network {

struct VersionUpdate {
    NodeLocation nodeLocation;
    NesPartition partition;
    DecomposedQueryPlanVersion version;
    ReconfigurationMarkerPtr marker;
};

NetworkSink::NetworkSink(const SchemaPtr& schema,
                         OperatorId uniqueNetworkSinkDescriptorId,
                         SharedQueryId sharedQueryId,
                         DecomposedQueryId decomposedQueryId,
                         DecomposedQueryPlanVersion decomposedQueryVersion,
                         const NodeLocation& destination,
                         NesPartition nesPartition,
                         Runtime::NodeEnginePtr nodeEngine,
                         size_t numOfProducers,
                         std::chrono::milliseconds waitTime,
                         uint8_t retryTimes,
                         uint64_t numberOfOrigins,
                         DecomposedQueryPlanVersion version,
                         OperatorId downstreamLogicalOperatorId)
    : SinkMedium(
        std::make_shared<NesFormat>(schema, NES::Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getBufferManager()),
        nodeEngine,
        numOfProducers,
        sharedQueryId,
        decomposedQueryId,
        decomposedQueryVersion,
        numberOfOrigins),
      uniqueNetworkSinkDescriptorId(uniqueNetworkSinkDescriptorId), nodeEngine(nodeEngine),
      networkManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getNetworkManager()),
      queryManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getQueryManager()), receiverLocation(destination),
      bufferManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getBufferManager()), nesPartition(nesPartition),
      messageSequenceNumber(0), numOfProducers(numOfProducers), waitTime(waitTime), retryTimes(retryTimes), version(version), downstreamOperatorId(downstreamLogicalOperatorId) {
    nodeEngine->initializeParentId(receiverLocation.getNodeId());
    NES_ASSERT(this->networkManager, "Invalid network manager");
    NES_DEBUG("NetworkSink: Created NetworkSink for partition {} location {}", nesPartition, destination.createZmqURI());
}

SinkMediumTypes NetworkSink::getSinkMediumType() { return SinkMediumTypes::NETWORK_SINK; }

bool NetworkSink::writeBufferedData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
    NES_TRACE("context {} writing data at sink {} on node {} for originId {} and seqNumber {}",
              workerContext.getId(),
              getUniqueNetworkSinkDescriptorId(),
              nodeEngine->getNodeId(),
              inputBuffer.getOriginId(),
              inputBuffer.getSequenceNumber());

    auto pair = workerContext.getNetworkChannel(getUniqueNetworkSinkDescriptorId());
    NES_DEBUG("retrieved pair from context")
    auto* channel = pair.first;
    auto receiver = pair.second;

    //if async establishing of connection is in process, do not attempt to send data
    if (channel == nullptr) {
        NES_DEBUG("channel is null returning")
        return false;
    }

    // auto receiver = static_cast<int64_t>(receiverLocation.getNodeId());
    auto parent = nodeEngine->getParentId();
    auto changeCount = nodeEngine->getParenChangeCount();
//    auto actualReconnectCount = workerContext.getReconnectCount(getDownstreamLogicalOperatorId());
    auto actualReconnectCount = reconnectCount;
    if (nodeEngine->isSimulatingBuffering() && (static_cast<int64_t>(receiver.getRawValue()) != parent || actualReconnectCount < changeCount)) {
        NES_ERROR("write buffered data: parent mismatch, do not unbuffer data. Version {}, Receiver: {}, parent: {}, parentChanges: {}, "
                  "actual reconnects: {}",
                  decomposedQueryVersion,
                  receiver,
                  parent,
                  changeCount,
                  actualReconnectCount);
        return false;
    }
    //todo 4228: check if buffers are actually sent and not only inserted into to send queue
    return channel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes(), ++messageSequenceNumber);
}

bool NetworkSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
    NES_DEBUG("context {} writing data at sink {} on node {} for originId {} and seqNumber {}",
              workerContext.getId(),
              getUniqueNetworkSinkDescriptorId(),
              nodeEngine->getNodeId(),
              inputBuffer.getOriginId(),
              inputBuffer.getSequenceNumber());

    //    NetworkChannel* channel = nullptr;
    //    if (workerContext.doesNetworkChannelExist(getUniqueNetworkSinkDescriptorId())) {
    //        channel = workerContext.getNetworkChannel(getUniqueNetworkSinkDescriptorId());
    //    }

    NES_DEBUG("retrieving channel")

    auto pair = workerContext.getNetworkChannel(getUniqueNetworkSinkDescriptorId());
    NES_DEBUG("retrived channel")
    auto* channel = pair.first;
    auto receiver = pair.second;

    //if async establishing of connection is in process, do not attempt to send data but buffer it instead
    //todo #4210: decrease amount of hashmap lookups
    if (!channel) {
        //todo #4311: check why sometimes buffers arrive after a channel has been closed
        NES_ASSERT2_FMT(workerContext.isAsyncConnectionInProgress(getUniqueNetworkSinkDescriptorId()),
                        "Trying to write to invalid channel while no connection is in progress");

        //check if connection was established and buffer it has not yest been established
        if (!retrieveNewChannelAndUnbuffer(workerContext)) {
            NES_TRACE("context {} buffering data", workerContext.getId());
            workerContext.insertIntoReconnectBufferStorage(getUniqueNetworkSinkDescriptorId(), inputBuffer);
            return true;
        }

        //if a connection was established, retrieve the channel
        pair = workerContext.getNetworkChannel(getUniqueNetworkSinkDescriptorId());
        channel = pair.first;
        receiver = pair.second;
    }
    unbuffer(workerContext);
    // auto receiver = static_cast<int64_t>(receiverLocation.getNodeId());
    auto parent = nodeEngine->getParentId();
    auto changeCount = nodeEngine->getParenChangeCount();
//    auto actualReconnectCount = workerContext.getReconnectCount(getDownstreamLogicalOperatorId());
    auto actualReconnectCount = reconnectCount;
    if (nodeEngine->isSimulatingBuffering() && (static_cast<int64_t>(receiver.getRawValue()) != parent || actualReconnectCount < changeCount)) {
        NES_ERROR("write buffered data: parent mismatch, do not unbuffer data. Version: {}, Receiver: {}, parent: {}, parentChanges: {}, "
                  "actual reconnects: {}",
                  decomposedQueryVersion,
                  receiver,
                  parent,
                  changeCount,
                  actualReconnectCount);
        workerContext.insertIntoReconnectBufferStorage(getUniqueNetworkSinkDescriptorId(), inputBuffer);
        return true;
    }

    NES_TRACE("Network Sink: {} data sent with sequence number {} successful", decomposedQueryId, messageSequenceNumber + 1);
    //todo 4228: check if buffers are actually sent and not only inserted into to send queue
    return channel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes(), ++messageSequenceNumber);
}

void NetworkSink::preSetup() {
    NES_DEBUG("NetworkSink: method preSetup() called {} qep {}", nesPartition.toString(), decomposedQueryId);
    if (!networkManager->registerSubpartitionEventConsumer(receiverLocation, nesPartition, inherited1::shared_from_this())) {
        NES_WARNING("Cannot register event listener {}", nesPartition.toString());
    }
}

void NetworkSink::setup() {
    NES_DEBUG("NetworkSink: method setup() called {} qep {}", nesPartition.toString(), decomposedQueryId);
    auto reconf = Runtime::ReconfigurationMessage(sharedQueryId,
                                                  decomposedQueryId,
                                                  version,
                                                  Runtime::ReconfigurationType::Initialize,
                                                  inherited0::shared_from_this(),
                                                  std::make_any<uint32_t>(numOfProducers));
    queryManager->addReconfigurationMessage(sharedQueryId, decomposedQueryId, version, reconf, true);
}

void NetworkSink::shutdown() {
    NES_DEBUG("shutdown() called {} queryId {} qepsubplan {}", nesPartition.toString(), sharedQueryId, decomposedQueryId);
    networkManager->unregisterSubpartitionProducer(nesPartition);
}

std::string NetworkSink::toString() const { return "NetworkSink: " + nesPartition.toString(); }

void NetworkSink::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& workerContext) {

    NES_DEBUG("NetworkSink: reconfigure() called {} qep {}", nesPartition.toString(), decomposedQueryId);
    inherited0::reconfigure(task, workerContext);
    Runtime::QueryTerminationType terminationType = Runtime::QueryTerminationType::Invalid;
    auto shouldPropagateMarker = true;
    std::optional<ReconfigurationMarkerPtr> reconfigurationMarker = std::nullopt;
    switch (task.getType()) {
        case Runtime::ReconfigurationType::Initialize: {
            //check if the worker is configured to use async connecting
            if (networkManager->getConnectSinksAsync()) {
                //async connecting is activated. Delegate connection process to another thread and start the future
                auto reconf = Runtime::ReconfigurationMessage(sharedQueryId,
                                                              decomposedQueryId,
                                                              version,
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
                auto pair = std::make_pair(std::make_pair(std::move(networkChannelFuture.first), receiverLocation.getNodeId()),
                                           std::move(networkChannelFuture.second));
                //workerContext.storeNetworkChannelFuture(getUniqueNetworkSinkDescriptorId(), std::move(networkChannelFuture));
                workerContext.storeNetworkChannelFuture(getUniqueNetworkSinkDescriptorId(), std::move(pair));
                workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), nullptr, INVALID_WORKER_NODE_ID);
//                if (nodeEngine->isSimulatingBuffering()) {
//                    workerContext.increaseReconnectCount(getDownstreamLogicalOperatorId(), receiverLocation.getNodeId());
//                }
            } else {
                //synchronous connecting is configured. let this thread wait on the connection being established
                auto channel = networkManager->registerSubpartitionProducer(receiverLocation,
                                                                            nesPartition,
                                                                            bufferManager,
                                                                            waitTime,
                                                                            retryTimes);
                NES_ASSERT(channel, "Channel not valid partition " << nesPartition);
                workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(),
                                                  std::move(channel),
                                                  receiverLocation.getNodeId());
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
        case Runtime::ReconfigurationType::BufferOutGoingTuples: {
            NES_NOT_IMPLEMENTED();
//            if (workerContext.isAsyncConnectionInProgress(getUniqueNetworkSinkDescriptorId())) {
//                //todo: make sure marker is propagated if connection happens
//                workerContext.abortConnectionProcess(getUniqueNetworkSinkDescriptorId());
//            }
//            workerContext.doNotTryConnectingDataChannel(getUniqueNetworkSinkDescriptorId());
//            //todo: we are just sending an empty marker hare to close the connection, the coordinator will then take care of the rest
//            auto marker = std::make_shared<ReconfigurationMarker>();
//            workerContext.releaseNetworkChannel(getUniqueNetworkSinkDescriptorId(),
//                                                Runtime::QueryTerminationType::Reconfiguration,
//                                                queryManager->getNumberOfWorkerThreads(),
//                                                messageSequenceNumber,
//                                                true,
//                                                marker);
//            workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), nullptr, INVALID_WORKER_NODE_ID);
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

            clearOldAndConnectToNewChannelAsync(workerContext,
                                                newReceiverLocation,
                                                newPartition,
                                                newVersion,
                                                versionUpdate.marker);
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
        case Runtime::ReconfigurationType::ReconfigurationMarker: {
            reconfigurationMarker = task.getUserData<ReconfigurationMarkerPtr>();
            auto event = reconfigurationMarker.value()->getReconfigurationEvent(
                DecomposedQueryIdWithVersion(decomposedQueryId, decomposedQueryVersion));
            NES_ASSERT2_FMT(event, "Markers should only be propagated to a network sink if the plan is to be reconfigured");

            if (event.value()->reconfigurationMetadata->instanceOf<DrainQueryMetadata>()) {
                terminationType = Runtime::reconfigurationTypeToTerminationType(task.getType());
            } else if (event.value()->reconfigurationMetadata->instanceOf<UpdateAndDrainQueryMetadata>()) {
                terminationType = Runtime::reconfigurationTypeToTerminationType(task.getType());
                shouldPropagateMarker = false;
            } else {
                NES_WARNING("Other reconfigurations not yes supported");
                NES_NOT_IMPLEMENTED();
            }
            break;
        }
        default: {
            break;
        }
    }
    if (terminationType != Runtime::QueryTerminationType::Invalid) {
        //todo #3013: make sure buffers are kept if the device is currently buffering
        NES_ERROR("shutting down network sink with termination type {}", magic_enum::enum_name(terminationType));
        if (workerContext.decreaseObjectRefCnt(this) == 1) {
            if (workerContext.isAsyncConnectionInProgress(getUniqueNetworkSinkDescriptorId())) {
                //todo #5159: make sure that downstream plans are garbage collected if they do not receive a drain
                //wait until channel has either connected or connection times out
                NES_DEBUG("NetworkSink: reconfigure() waiting for channel to connect in order to unbuffer before shutdown. "
                          "operator {} Thread {}",
                          nesPartition.toString(),
                          Runtime::NesThread::getId());
                //todo #4309: make sure this function times out
                //the following call blocks until the channel has been established
                auto channel = workerContext.waitForAsyncConnection(getUniqueNetworkSinkDescriptorId(), retryTimes);
                if (channel.first) {
                    NES_DEBUG("NetworkSink: reconfigure() established connection for operator {} Thread {}",
                              nesPartition.toString(),
                              Runtime::NesThread::getId());
                    workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(),
                                                      std::move(channel.first), channel.second);
                } else {
                    networkManager->unregisterSubpartitionProducer(nesPartition);
                    //do not release network channel in the next step because none was established
                    NES_ERROR("shutting down network sink done without connect: {}", magic_enum::enum_name(terminationType));
                    return;
                }
            }
            if (terminationType != Runtime::QueryTerminationType::HardStop) {
                unbuffer(workerContext);
            }
            networkManager->unregisterSubpartitionProducer(nesPartition);
            NES_ASSERT2_FMT(workerContext.releaseNetworkChannel(getUniqueNetworkSinkDescriptorId(),
                                                                terminationType,
                                                                queryManager->getNumberOfWorkerThreads(),
                                                                messageSequenceNumber,
                                                                shouldPropagateMarker,
                                                                reconfigurationMarker),
                            "Cannot remove network channel " << nesPartition.toString());
            /* store a nullptr in place of the released channel, in case another write happens afterwards, that will prevent crashing and
            allow throwing an error instead */
            workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), nullptr, INVALID_WORKER_NODE_ID);
            NES_DEBUG("NetworkSink: reconfigure() released channel on {} Thread {}",
                      nesPartition.toString(),
                      Runtime::NesThread::getId());
            NES_ERROR("shutting down network sink done with connect: {}", magic_enum::enum_name(terminationType));
        }
    }
}

void NetworkSink::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    NES_DEBUG("NetworkSink: postReconfigurationCallback() called {} parent plan {}", nesPartition.toString(), decomposedQueryId);
    inherited0::postReconfigurationCallback(task);

    switch (task.getType()) {
        //update info about receiving network source to new target
        case Runtime::ReconfigurationType::ConnectToNewReceiver: {
            auto versionUpdate = task.getUserData<VersionUpdate>();
            networkManager->unregisterSubpartitionProducer(nesPartition);

            receiverLocation = versionUpdate.nodeLocation;
            nesPartition = versionUpdate.partition;
            version = versionUpdate.version;
            decomposedQueryVersion = versionUpdate.version;
            messageSequenceNumber = 0;

            break;
        }
        case Runtime::ReconfigurationType::ReconfigurationMarker: {
            auto reconfigurationMarker = task.getUserData<ReconfigurationMarkerPtr>();
            auto event = reconfigurationMarker->getReconfigurationEvent(
                DecomposedQueryIdWithVersion(decomposedQueryId, decomposedQueryVersion));
            NES_ASSERT2_FMT(event, "Markers should only be propagated to a network sink if the plan is to be reconfigured");
            auto metadata = event.value()->reconfigurationMetadata;
            if (metadata->instanceOf<UpdateAndDrainQueryMetadata>()) {
                // sink should start new plan
                auto updateAndDrain = metadata->as<UpdateAndDrainQueryMetadata>();
                auto planToRegisterAndStart =
                    nodeEngine->checkDecomposableQueryPlanToStart(updateAndDrain->decomposedQueryId,
                                                                  updateAndDrain->decomposedQueryPlanVersion);
                // if plan id is the same, new plan should be registered first
                if (decomposedQueryId == updateAndDrain->decomposedQueryId && planToRegisterAndStart) {
                    // !Registering the plan is blocking operation, therefore it should be started from different thread!
                    std::thread thread([nodeEngine = nodeEngine,
                                        queryManager = queryManager,
                                        reconfigurationMarker = reconfigurationMarker,
                                        planToRegisterAndStart = planToRegisterAndStart,
                                        decomposedQueryId = decomposedQueryId,
                                        decomposedQueryVersion = decomposedQueryVersion,
                    replay = getReplayData()]() mutable {
                        // register new plan
                        NES_DEBUG("Registering new decomposed query plan with id {}.{}",
                                  decomposedQueryId,
                                  decomposedQueryVersion);
                        auto registered = nodeEngine->registerExecutableQueryPlan(planToRegisterAndStart, replay);
                        if (registered) {
                            // start new plan
                            NES_DEBUG("Starting new decomposed query plan with id {}.{}",
                                      decomposedQueryId,
                                      decomposedQueryVersion);
                            if (!queryManager->startNewExecutableQueryPlanAndPropagateMarker(
                                    reconfigurationMarker,
                                    DecomposedQueryIdWithVersion(decomposedQueryId, decomposedQueryVersion))) {
                                NES_ERROR("Plan {}.{} was not started, failure occurred",
                                          decomposedQueryId,
                                          decomposedQueryVersion);
                            }
                        } else {
                            NES_ERROR("Could not start migration plan {}.{} Registration of new plan failed",
                                      decomposedQueryId,
                                      decomposedQueryVersion);
                        }
                    });

                    thread.detach();
                } else if (decomposedQueryId != updateAndDrain->decomposedQueryId) {
                    // start new plan
                    NES_DEBUG("Starting new decomposed query plan with id {}.{}",
                              updateAndDrain->decomposedQueryId,
                              updateAndDrain->decomposedQueryPlanVersion);
                    if (!queryManager->startNewExecutableQueryPlanAndPropagateMarker(
                            reconfigurationMarker,
                            DecomposedQueryIdWithVersion(decomposedQueryId, decomposedQueryVersion))) {
                        NES_ERROR("Plan {}.{} was not started, failure occurred", decomposedQueryId, decomposedQueryVersion);
                    }
                } else {
                    NES_ERROR("Could not start migration plan {}.{} Plan not found in delayed",
                              decomposedQueryId,
                              decomposedQueryVersion);
                }
                NES_DEBUG("finished adding new decomposed query plan");
            }

            if (!event.value()->reconfigurationMetadata->instanceOf<DrainQueryMetadata>()
                && !event.value()->reconfigurationMetadata->instanceOf<UpdateAndDrainQueryMetadata>()) {
                NES_WARNING("Non drain reconfigurations not yes supported");
                NES_NOT_IMPLEMENTED();
            }
            break;
        }
        default: {
            break;
        }
    }
}

void NetworkSink::onEvent(Runtime::BaseEvent& event) {
    NES_DEBUG("NetworkSink::onEvent(event) called. uniqueNetworkSinkDescriptorId: {}", this->uniqueNetworkSinkDescriptorId);
    auto qep = queryManager->getQueryExecutionPlan(DecomposedQueryIdWithVersion(decomposedQueryId, version));
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

void NetworkSink::configureNewSinkDescriptor(const NetworkSinkDescriptor& newNetworkSinkDescriptor,
                                             ReconfigurationMarkerPtr marker) {
    auto newReceiverLocation = newNetworkSinkDescriptor.getNodeLocation();
    auto newPartition = newNetworkSinkDescriptor.getNesPartition();
    auto newVersion = newNetworkSinkDescriptor.getVersion();
    VersionUpdate newReceiverTuple = {newReceiverLocation, newPartition, newVersion, std::move(marker)};
    //register event consumer for new source. It has to be registered before any data channels connect
    NES_ASSERT2_FMT(
        networkManager->registerSubpartitionEventConsumer(newReceiverLocation, newPartition, inherited1::shared_from_this()),
        "Cannot register event listener " << nesPartition.toString());
    Runtime::ReconfigurationMessage message = Runtime::ReconfigurationMessage(nesPartition.getQueryId(),
                                                                              decomposedQueryId,
                                                                              version,
                                                                              Runtime::ReconfigurationType::ConnectToNewReceiver,
                                                                              inherited0::shared_from_this(),
                                                                              newReceiverTuple);
    queryManager->addReconfigurationMessage(nesPartition.getQueryId(), decomposedQueryId, version, message, false);
}

void NetworkSink::clearOldAndConnectToNewChannelAsync(Runtime::WorkerContext& workerContext,
                                                      const NodeLocation& newNodeLocation,
                                                      NesPartition newNesPartition,
                                                      DecomposedQueryPlanVersion newVersion,
                                                      const std::optional<ReconfigurationMarkerPtr>& reconfigurationMarker) {
    NES_DEBUG("NetworkSink: method clearOldAndConnectToNewChannelAsync() called {} qep {}, by thread {}",
              nesPartition.toString(),
              decomposedQueryId,
              Runtime::NesThread::getId());

    networkManager->unregisterSubpartitionProducer(nesPartition);

    auto reconf = Runtime::ReconfigurationMessage(sharedQueryId,
                                                  decomposedQueryId,
                                                  version,
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
    auto pair = std::make_pair(std::make_pair(std::move(networkChannelFuture.first), newNodeLocation.getNodeId()),
                               std::move(networkChannelFuture.second));
    //todo: #4282 use QueryTerminationType::Redeployment
    // workerContext.storeNetworkChannelFuture(getUniqueNetworkSinkDescriptorId(), std::move(networkChannelFuture));
    workerContext.storeNetworkChannelFuture(getUniqueNetworkSinkDescriptorId(), std::move(pair));
    //todo #4310: do release and storing of nullptr in one call

    auto terminationType = reconfigurationMarker.has_value() ? Runtime::QueryTerminationType::Reconfiguration
                                                             : Runtime::QueryTerminationType::Graceful;
    workerContext.releaseNetworkChannel(getUniqueNetworkSinkDescriptorId(),
                                        terminationType,
                                        queryManager->getNumberOfWorkerThreads(),
                                        messageSequenceNumber,
                                        true,
                                        reconfigurationMarker);
    workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), nullptr, INVALID_WORKER_NODE_ID);
//    workerContext.increaseReconnectCount(getDownstreamLogicalOperatorId(), newNodeLocation.getNodeId());
}

void NetworkSink::unbuffer(Runtime::WorkerContext& workerContext) {
    auto topBuffer = workerContext.peekBufferFromReconnectBufferStorage(getUniqueNetworkSinkDescriptorId());

    NES_INFO("sending buffered data");
    auto numBuffers = 0;
    while (topBuffer && topBuffer.value().getBuffer()) {
        if (!writeBufferedData(topBuffer.value(), workerContext)) {
            NES_DEBUG("could not send all data from buffer");
            break;
        }
        workerContext.removeBufferFromReconnectBufferStorage(getUniqueNetworkSinkDescriptorId());
        NES_TRACE("buffer sent");
        topBuffer = workerContext.peekBufferFromReconnectBufferStorage(getUniqueNetworkSinkDescriptorId());
        numBuffers++;
    }
    NES_DEBUG("Sent {} buffers to node {}", numBuffers, nodeEngine->getParentId());
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
    if (newNetworkChannelFutureOptional.value().first == nullptr) {
        NES_DEBUG("NetworkSink: reconfigure() network channel retrieved from future is null for operator {} Thread {}",
                  nesPartition.toString(),
                  Runtime::NesThread::getId());
        NES_ASSERT2_FMT(retryTimes != 0, "Received invalid channel although channel retry times are set to indefinite");

        //todo 4308: if there were finite retry attempts and they failed, do not crash the worker but fail the query
        NES_ASSERT2_FMT(false, "Could not establish network channel");
        return false;
    }
    workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(),
                                      std::move(newNetworkChannelFutureOptional.value().first),
                                      newNetworkChannelFutureOptional.value().second);
//    workerContext.increaseReconnectCount(getUniqueNetworkSinkDescriptorId());
    nodeEngine->setParentIdIfInvalid(newNetworkChannelFutureOptional.value().second);
    NES_DEBUG("stop buffering data for context {}", workerContext.getId());
    unbuffer(workerContext);
    return true;
}

//bool NetworkSink::startBuffering(DecomposedQueryId decomposedQueryPlanId, DecomposedQueryPlanVersion decomposedQueryPlanVersion) {
//    Runtime::ReconfigurationMessage message = Runtime::ReconfigurationMessage(nesPartition.getQueryId(),
//                                                                              decomposedQueryPlanId,
//                                                                                decomposedQueryPlanVersion,
//                                                                              Runtime::ReconfigurationType::BufferOutGoingTuples,
//                                                                              inherited0::shared_from_this());
//    return queryManager->addReconfigurationMessage(nesPartition.getQueryId(), decomposedQueryPlanId, decomposedQueryPlanVersion, message, false);
//}

WorkerId NetworkSink::getReceiverId() { return receiverLocation.getNodeId(); }

bool NetworkSink::checkParentDiff(int64_t receiver, int64_t parent) {
    // NES_ERROR("checking parent diff")
    // NES_ERROR("receiver {}, parent{}", receiver, parent)
    auto res = (parent == receiver - 1) || (receiver == 2 && parent == 11);
    // NES_ERROR("receiver {}, parent{}, result {}", receiver, parent, res);
    return res;
}

OperatorId NetworkSink::getDownstreamLogicalOperatorId() { return downstreamOperatorId; }

void NetworkSink::setReconnectCount(uint64_t count) {
    NES_ERROR("Setting reconnect count for sink {} from {} to {}", uniqueNetworkSinkDescriptorId, reconnectCount, count);
    reconnectCount = count;
}
}// namespace NES::Network
