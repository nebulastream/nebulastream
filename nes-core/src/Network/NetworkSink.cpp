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
#include <Util/Logger/Logger.hpp>

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
                         uint64_t numberOfOrigins,
                         bool isBuffering)
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
    if ((faultToleranceType != FaultToleranceType::NONE)
        && isBuffering) {
        std::cout << "Create Network Sink\n";
        insertIntoStorageCallback = [this](Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
            workerContext.insertIntoStorage(this->nesPartition, inputBuffer);
        };
        deleteFromStorageCallback = [this](EpochMessage epochMessage, Runtime::WorkerContext& workerContext) {
            workerContext.trimStorage(this->nesPartition, epochMessage.getTimestamp(), epochMessage.getReplicationLevel());
        };
    } else {
        insertIntoStorageCallback = [](Runtime::TupleBuffer&, Runtime::WorkerContext&) {
        };
        deleteFromStorageCallback = [](EpochMessage, Runtime::WorkerContext&) {
        };
    }
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
//        auto now = std::chrono::system_clock::now();
//        auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
//        auto epoch = now_ms.time_since_epoch();
//        auto value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
//        auto ts = std::chrono::system_clock::now();
//        auto timeNow = std::chrono::system_clock::to_time_t(ts);
//        inputBuffer.setCreationTimestampInMS(value.count());
        auto success = channel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes());
        if (success) {
            insertIntoStorageCallback(inputBuffer, workerContext);
        }
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
            auto channel =
                networkManager->registerSubpartitionProducer(receiverLocation, nesPartition, bufferManager, waitTime, retryTimes);
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
            //on arrival of an epoch barrier trim data in buffer storages in network sinks that belong to one query plan
            auto epochMessage = task.getUserData<EpochMessage>();
            deleteFromStorageCallback(epochMessage, workerContext);
            break;
        }
        case Runtime::ResendData: {
            NES_DEBUG("NetworkSink: ResendData called " << nesPartition.toString() << " parent plan " << querySubPlanId);
            auto now = std::chrono::system_clock::now();
            auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
            auto epoch = now_ms.time_since_epoch();
            auto value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
            auto ts = std::chrono::system_clock::now();
            auto timeNow = std::chrono::system_clock::to_time_t(ts);
            std::cout << "Start resending data" << std::put_time(std::localtime(&timeNow), "%Y-%m-%d %X");
            auto tuplesToResend = workerContext.resendDataFromDataStorage(this->nesPartition);
            while (!tuplesToResend.empty()) {
                auto tupleBuffer = tuplesToResend.top();
                writeData(tupleBuffer, workerContext);
                tuplesToResend.pop();
            }
            now = std::chrono::system_clock::now();
            now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
            epoch = now_ms.time_since_epoch();
            value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
            ts = std::chrono::system_clock::now();
            timeNow = std::chrono::system_clock::to_time_t(ts);
            std::cout << "End resending data" << std::put_time(std::localtime(&timeNow), "%Y-%m-%d %X");
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

    if (event.getEventType() == Runtime::EventType::kStartSourceEvent) {
        // todo jm continue here. how to obtain local worker context?
    } else if (event.getEventType() == Runtime::EventType::kCustomEvent) {
        auto epochEvent = dynamic_cast<Runtime::CustomEventWrapper&>(event).data<Runtime::PropagateEpochEvent>();
        auto epochBarrier = epochEvent->timestampValue();
        auto propagationDelay = epochEvent->propagationDelayValue();
        auto success = queryManager->sendTrimmingReconfiguration(querySubPlanId, epochBarrier, propagationDelay);
        if (success) {
            NES_DEBUG("NetworkSink::onEvent: epoch" << epochBarrier << " queryId " << queryId << " trimmed");
            success = queryManager->propagateEpochBackwards(querySubPlanId, epochBarrier, propagationDelay);
            if (success) {
                NES_DEBUG("NetworkSink::onEvent: epoch" << epochBarrier << " queryId " << queryId << " sent further");
            } else {
                NES_INFO("NetworkSink::onEvent:: end of propagation " << epochBarrier << " queryId " << queryId);
            }
        } else {
            NES_ERROR("NetworkSink::onEvent:: could not trim " << epochBarrier << " queryId " << queryId);
        }
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
