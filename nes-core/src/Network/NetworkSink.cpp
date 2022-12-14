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
                         FaultToleranceType faultToleranceType,
                         uint64_t numberOfOrigins,
                         bool isLeaf)
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
      numOfProducers(numOfProducers), waitTime(waitTime), retryTimes(retryTimes) {
    NES_ASSERT(this->networkManager, "Invalid network manager");
    NES_DEBUG("NetworkSink: Created NetworkSink for partition " << nesPartition << " location " << destination.createZmqURI());
    if (faultToleranceType != FaultToleranceType::NONE) {
        insertIntoStorageCallback = [this](Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
            workerContext.insertIntoStorage(this->nesPartition, inputBuffer);
        };
        sendPropagationCallback = [](Runtime::TupleBuffer&) {};
        trimmingCallback = [this](uint64_t epochBarrier) {
            queryManager->sendTrimmingReconfiguration(this->querySubPlanId, epochBarrier);
        };
        if (faultToleranceType == FaultToleranceType::AT_MOST_ONCE) {
            sendPropagationCallback = [this](Runtime::TupleBuffer& inputBuffer) {
                if (!(bufferCount % buffersPerEpoch) && bufferCount != 0) {
                    auto epochBarrier = inputBuffer.getWatermark();
                    auto success = queryManager->propagateKEpochBackwards(this->querySubPlanId, epochBarrier, this->replicationLevel);
                    if (success) {
                        NES_DEBUG("NetworkSink::writeData: epoch" << epochBarrier << " queryId " << this->queryId << " sent back");
                    } else {
                        NES_ERROR("NetworkSink::writeData:: couldn't send " << epochBarrier << " queryId " << this->queryId);
                    }
                }
            };
        }
        else if (faultToleranceType == FaultToleranceType::EXACTLY_ONCE) {
            if (!isLeaf) {
                insertIntoStorageCallback = [](Runtime::TupleBuffer&, Runtime::WorkerContext&) {};
                trimmingCallback = [](uint64_t) {};
            }
        }
    }
    else {
        insertIntoStorageCallback = [](Runtime::TupleBuffer&, Runtime::WorkerContext&) {};
        trimmingCallback = [](uint64_t) {};
    }
    bufferCount = 0;
}

SinkMediumTypes NetworkSink::getSinkMediumType() { return NETWORK_SINK; }

bool NetworkSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
    auto* channel = workerContext.getNetworkChannel(nesPartition.getOperatorId());
    if (channel) {
        insertIntoStorageCallback(inputBuffer, workerContext);
        auto now = std::chrono::system_clock::now();
        auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
        auto epoch = now_ms.time_since_epoch();
        auto value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
        inputBuffer.setCreationTimestamp(value.count());
        auto success = channel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes());
        sendPropagationCallback(inputBuffer);
        bufferCount++;
        return success;
    }
    NES_ASSERT2_FMT(false, "invalid channel on " << nesPartition);
    return false;
}

bool NetworkSink::resendData(Runtime::WorkerContext& workerContext) {
    auto* channel = workerContext.getNetworkChannel(nesPartition.getOperatorId());
    auto queue = workerContext.resendBuffers(nesPartition);
    if (channel) {
        while (!queue.empty()) {
            auto buffer = queue.top();
            auto success = channel->sendBuffer(buffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes());
            if (!success) {
                NES_ERROR("NetworkSink: Couldn't resend tuples");
            }
            queue.pop();
        }
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
//            //on arrival of an epoch barrier trim data in buffer storages in network sinks that belong to one query plan
            auto epochMessage = task.getUserData<EpochMessage>();
//            NES_DEBUG("Executing PropagateEpoch on qep queryId=" << queryId
//                                                                 << "punctuation= " << timestamp);
//            channel->sendEvent<Runtime::PropagateEpochEvent>(Runtime::EventType::kCustomEvent, timestamp, queryId);
            workerContext.printPropagationDelay(epochMessage.getAdditionalInfo());
            workerContext.trimStorage(nesPartition, epochMessage.getTimestamp());
            break;
        }
        case Runtime::RedirectOutput: {
            NES_DEBUG("NetworkSink: Updating NetworkSink: " << nesPartition.toString());
            workerContext.releaseNetworkChannel(nesPartition.getOperatorId(), Runtime::QueryTerminationType::Redirect);
            NodeLocation newNodeLocation = task.getUserData<NodeLocation>();
            Network::NodeLocation updatedNodeLocation(newNodeLocation.getNodeId(), newNodeLocation.getHostname(), newNodeLocation.getPort());
            receiverLocation = updatedNodeLocation;

            auto updatedChannel =
                networkManager->registerSubpartitionProducer(updatedNodeLocation, nesPartition, bufferManager, waitTime, retryTimes);
            workerContext.updateNetworkChannel(nesPartition.getOperatorId(), std::move(updatedChannel));
            resendData(workerContext);
            auto ts = std::chrono::system_clock::now();
            auto time = std::chrono::duration_cast<std::chrono::nanoseconds>(ts.time_since_epoch()).count();
            NES_DEBUG("NetworkSink: Migration Complete; time : " << time);
            break;
        }
        default: {
            break;
        }
    }
    if (terminationType != Runtime::QueryTerminationType::Invalid) {
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
    auto qep = queryManager->getQueryExecutionPlan(querySubPlanId);
    qep->onEvent(event);

    NES_DEBUG("NetworkSink: received an event");
    if (event.getEventType() == Runtime::EventType::kCustomEvent) {
        auto epochEvent = dynamic_cast<Runtime::CustomEventWrapper&>(event).data<Runtime::PropagateEpochEvent>();
        auto epochBarrier = epochEvent->timestampValue();
        auto originalTimestamp = epochEvent->creationTimestampValue();
        trimmingCallback(epochBarrier);
        NES_DEBUG("NetworkSink::onEvent: epoch" << epochBarrier << " queryId " << this->queryId << " trimmed");
        auto success = queryManager->propagateEpochBackwards(this->querySubPlanId, epochBarrier, originalTimestamp);
        if (success) {
            NES_DEBUG("NetworkSink::onEvent: epoch" << epochBarrier << " queryId " << this->queryId << " sent further");
        } else {
            NES_INFO("NetworkSink::onEvent:: end of propagation " << epochBarrier << " queryId " << this->queryId);
        }
    }
    else {
        auto epochEvent = dynamic_cast<Runtime::KEpochEventWrapper&>(event).data<Runtime::PropagateKEpochEvent>();
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
            return;
        }
        else {
            trimmingCallback(epochBarrier);
            NES_DEBUG("NetworkSink::onEvent: epoch" << epochBarrier << " queryId " << queryId << " trimmed");
            auto success = queryManager->propagateKEpochBackwards(querySubPlanId, epochBarrier, 0);
            if (success) {
                NES_DEBUG("NetworkSink::onEvent: epoch" << epochBarrier << " queryId " << queryId << " sent further");
            }
            else {
                NES_INFO("NetworkSink::onEvent:: end of propagation " << epochBarrier << " queryId " << queryId);
            }
        }
    }
}

OperatorId NetworkSink::getUniqueNetworkSinkDescriptorId() { return uniqueNetworkSinkDescriptorId; }

Runtime::NodeEnginePtr NetworkSink::getNodeEngine() { return nodeEngine; }

}// namespace NES::Network
