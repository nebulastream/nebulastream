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
                         FaultToleranceType faultToleranceType)
    : SinkMedium(
        std::make_shared<NesFormat>(schema, NES::Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getBufferManager()),
        nodeEngine,
        numOfProducers,
        sharedQueryId,
        decomposedQueryId,
        decomposedQueryVersion,
faultToleranceType,
        numberOfOrigins),
      uniqueNetworkSinkDescriptorId(uniqueNetworkSinkDescriptorId), nodeEngine(nodeEngine),
      networkManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getNetworkManager()),
      queryManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getQueryManager()), receiverLocation(destination),
      bufferManager(Util::checkNonNull(nodeEngine, "Invalid Node Engine")->getBufferManager()), nesPartition(nesPartition),
      messageSequenceNumber(0), numOfProducers(numOfProducers), waitTime(waitTime), retryTimes(retryTimes), version(version),
      trimmingStarted(false) {
    NES_ASSERT(this->networkManager, "Invalid network manager");
    NES_DEBUG("NetworkSink: Created NetworkSink for partition {} location {}", nesPartition, destination.createZmqURI());
    if (faultToleranceType == FaultToleranceType::AS || faultToleranceType == FaultToleranceType::UB) {
        insertIntoStorageCallback = [this](Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
            workerContext.insertIntoStorage(this->nesPartition, inputBuffer);
        };
        deleteFromStorageCallback = [this](uint64_t timestamp, Runtime::WorkerContext& workerContext) {
            return workerContext.trimStorage(this->nesPartition, timestamp);
        };
    }
    else if (faultToleranceType == FaultToleranceType::CH) {
        insertIntoStorageCallback = [this](Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
            workerContext.insertIntoStorage(this->nesPartition, inputBuffer);
            // workerContext.createCheckpoint(nesPartition, inputBuffer);  // uses HDFS
            if (this->bufferCount == 50) {
                std::vector<char> binaryStorage = workerContext.getBinaryStorage(this->nesPartition);
                this->nodeEngine->offloadCheckpoint(this->nesPartition.getPartitionId().getRawValue(), binaryStorage);
            }
        };
        deleteFromStorageCallback = [this](uint64_t timestamp, Runtime::WorkerContext& workerContext) {
            // workerContext.trimCheckpoint(nesPartition, timestamp);  // uses HDFS
            this->nodeEngine->rpcTrimCheckpoint(this->nesPartition.getPartitionId().getRawValue(), timestamp);
            return workerContext.trimStorage(this->nesPartition, timestamp);
        };
    }
    else {
        insertIntoStorageCallback = [](Runtime::TupleBuffer&, Runtime::WorkerContext&) {
        };
        deleteFromStorageCallback = [](uint64_t, Runtime::WorkerContext&) {
            return false;
        };
    }
    bufferCount = 0;
}

SinkMediumTypes NetworkSink::getSinkMediumType() { return SinkMediumTypes::NETWORK_SINK; }

bool NetworkSink::writeBufferedData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
    NES_TRACE("context {} writing data at sink {} on node {} for originId {} and seqNumber {}",
              workerContext.getId(),
              getUniqueNetworkSinkDescriptorId(),
              nodeEngine->getNodeId(),
              inputBuffer.getOriginId(),
              inputBuffer.getSequenceNumber());

    auto channel = workerContext.getNetworkChannel(getUniqueNetworkSinkDescriptorId(), decomposedQueryVersion);

    //if async establishing of connection is in process, do not attempt to send data
    if (channel == nullptr) {
        return false;
    }

    return channel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes(), ++messageSequenceNumber);
}

bool NetworkSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
    NES_DEBUG("context {} writing data at sink {} on node {} for originId {} and seqNumber {}",
              workerContext.getId(),
              getUniqueNetworkSinkDescriptorId(),
              nodeEngine->getNodeId(),
              inputBuffer.getOriginId(),
              inputBuffer.getSequenceNumber());

    auto* channel = workerContext.getNetworkChannel(getUniqueNetworkSinkDescriptorId(), decomposedQueryVersion);

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
        channel = workerContext.getNetworkChannel(getUniqueNetworkSinkDescriptorId(), decomposedQueryVersion);
    }

    NES_TRACE("Network Sink: {} data sent with sequence number {} successful", decomposedQueryId, messageSequenceNumber + 1);
    //todo 4228: check if buffers are actually sent and not only inserted into to send queue
    insertIntoStorageCallback(inputBuffer, workerContext);
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
                workerContext.storeNetworkChannelFuture(getUniqueNetworkSinkDescriptorId(),
                                                        decomposedQueryVersion,
                                                        std::move(networkChannelFuture));
                workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), decomposedQueryVersion, nullptr);
            } else {
                //synchronous connecting is configured. let this thread wait on the connection being established
                auto channel = networkManager->registerSubpartitionProducer(receiverLocation,
                                                                            nesPartition,
                                                                            bufferManager,
                                                                            waitTime,
                                                                            retryTimes);
                NES_ASSERT(channel, "Channel not valid partition " << nesPartition);
                workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), decomposedQueryVersion, std::move(channel));
                NES_DEBUG("NetworkSink: reconfigure() stored channel on {} Thread {} ref cnt {}",
                          nesPartition.toString(),
                          Runtime::NesThread::getId(),
                          task.getUserData<uint32_t>());
            }
            workerContext.setObjectRefCnt(this, task.getUserData<uint32_t>());
            workerContext.storeSchema(nesPartition, SinkMedium::getSchemaPtr());
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
        case Runtime::ReconfigurationType::PropagateEpoch: {
            //on arrival of an epoch barrier trim data in buffer storages in network sinks that belong to one query plan
            auto timestamp = task.getUserData<uint64_t>();
            NES_DEBUG("Executing PropagateEpoch on qep queryId={} punctuation={} ", decomposedQueryId, timestamp);
            deleteFromStorageCallback(timestamp, workerContext);
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
                auto channel = workerContext.waitForAsyncConnection(getUniqueNetworkSinkDescriptorId());
                if (channel) {
                    NES_DEBUG("NetworkSink: reconfigure() established connection for operator {} Thread {}",
                              nesPartition.toString(),
                              Runtime::NesThread::getId());
                    workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(),
                                                      decomposedQueryVersion,
                                                      std::move(channel));
                } else {
                    networkManager->unregisterSubpartitionProducer(nesPartition);
                    //do not release network channel in the next step because none was established
                    return;
                }
            }
            unbuffer(workerContext);
            networkManager->unregisterSubpartitionProducer(nesPartition);
            NES_ASSERT2_FMT(workerContext.releaseNetworkChannel(getUniqueNetworkSinkDescriptorId(),
                                                                decomposedQueryVersion,
                                                                terminationType,
                                                                queryManager->getNumberOfWorkerThreads(),
                                                                messageSequenceNumber,
                                                                shouldPropagateMarker,
                                                                reconfigurationMarker),
                            "Cannot remove network channel " << nesPartition.toString());
            /* store a nullptr in place of the released channel, in case another write happens afterwards, that will prevent crashing and
            allow throwing an error instead */
            workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), decomposedQueryVersion, nullptr);
            NES_DEBUG("NetworkSink: reconfigure() released channel on {} Thread {}",
                      nesPartition.toString(),
                      Runtime::NesThread::getId());
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
                                        decomposedQueryVersion = decomposedQueryVersion]() mutable {
                        // register new plan
                        NES_DEBUG("Registering new decomposed query plan with id {}.{}",
                                  decomposedQueryId,
                                  decomposedQueryVersion);
                        auto registered = nodeEngine->registerExecutableQueryPlan(planToRegisterAndStart);
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
    NES_DEBUG("NetworkSink::onEvent(event, wrkContext) called. uniqueNetworkSinkDescriptorId: {}",
              this->uniqueNetworkSinkDescriptorId);
    if (event.getEventType() == Runtime::EventType::kCustomEvent) {
        auto epochEvent = dynamic_cast<Runtime::CustomEventWrapper&>(event).data<Runtime::PropagateEpochEvent>();
        auto epochBarrier = epochEvent->timestampValue();
        auto success = queryManager->sendTrimmingReconfiguration(decomposedQueryId, epochBarrier);
        if (success) {
            success = queryManager->propagateEpochBackwards(decomposedQueryId, epochBarrier);
            if (success) {
                NES_DEBUG("NetworkSink::onEvent: epoch {} queryId {} trimmed", epochBarrier, decomposedQueryId);
            } else {
                NES_INFO("NetworkSink::onEvent end of propagation: epoch {} queryId {}", epochBarrier, decomposedQueryId);
            }
        } else {
            NES_ERROR("NetworkSink::onEvent:: could not trim : epoch {} queryId {}", epochBarrier, decomposedQueryId);
        }
    }
}

void NetworkSink::onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef) {
    NES_DEBUG("NetworkSink::onEvent(event, wrkContext) called. uniqueNetworkSinkDescriptorId: {}",
              this->uniqueNetworkSinkDescriptorId);
    if (event.getEventType() == Runtime::EventType::kCustomEvent) {
        auto epochEvent = dynamic_cast<Runtime::CustomEventWrapper&>(event).data<Runtime::PropagateEpochEvent>();
        auto epochBarrier = epochEvent->timestampValue();
        auto success = queryManager->sendTrimmingReconfiguration(decomposedQueryId, epochBarrier);
        if (success) {
            success = queryManager->propagateEpochBackwards(decomposedQueryId, epochBarrier);
            if (success) {
                NES_DEBUG("NetworkSink::onEvent: epoch {} queryId {} trimmed", epochBarrier, decomposedQueryId);
            } else {
                NES_INFO("NetworkSink::onEvent end of propagation: epoch {} queryId {}", epochBarrier, decomposedQueryId);
            }
        } else {
            NES_ERROR("NetworkSink::onEvent:: could not trim : epoch {} queryId {}", epochBarrier, decomposedQueryId);
        }
    }
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
    //todo: #4282 use QueryTerminationType::Redeployment
    workerContext.storeNetworkChannelFuture(getUniqueNetworkSinkDescriptorId(),
                                            decomposedQueryVersion,
                                            std::move(networkChannelFuture));
    //todo #4310: do release and storing of nullptr in one call

    auto terminationType = reconfigurationMarker.has_value() ? Runtime::QueryTerminationType::Reconfiguration
                                                             : Runtime::QueryTerminationType::Graceful;
    workerContext.releaseNetworkChannel(getUniqueNetworkSinkDescriptorId(),
                                        decomposedQueryVersion,
                                        terminationType,
                                        queryManager->getNumberOfWorkerThreads(),
                                        messageSequenceNumber,
                                        true,
                                        reconfigurationMarker);
    workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(), decomposedQueryVersion, nullptr);
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
}

bool NetworkSink::retrieveNewChannelAndUnbuffer(Runtime::WorkerContext& workerContext) {
    //retrieve new channel
    auto newNetworkChannelFutureOptional =
        workerContext.getAsyncConnectionResult(getUniqueNetworkSinkDescriptorId(), decomposedQueryVersion);

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
    workerContext.storeNetworkChannel(getUniqueNetworkSinkDescriptorId(),
                                      decomposedQueryVersion,
                                      std::move(newNetworkChannelFutureOptional.value()));

    NES_INFO("stop buffering data for context {}", workerContext.getId());
    unbuffer(workerContext);
    return true;
}

bool NetworkSink::hasTrimmed() { return trimmingStarted; }
}// namespace NES::Network
