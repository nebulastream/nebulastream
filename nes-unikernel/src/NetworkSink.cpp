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
#include <Runtime/NesThread.hpp>
// #include <Runtime/NodeEngine.hpp>
#include <Runtime/WorkerContext.hpp>

#include <Runtime/ReconfigurationMessage.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>

extern NES::Network::NetworkManagerPtr TheNetworkManager;
extern NES::Runtime::BufferManagerPtr TheBufferManager;
extern NES::Runtime::WorkerContextPtr TheWorkerContext;
namespace NES::Network {

NetworkSink::NetworkSink(OperatorId uniqueNetworkSinkDescriptorId,
                         SharedQueryId queryId,
                         DecomposedQueryPlanId querySubPlanId,
                         const NodeLocation& destination,
                         NesPartition nesPartition,
                         size_t outputSchemaSizeInBytes,
                         size_t numOfProducers,
                         std::chrono::milliseconds waitTime,
                         uint8_t retryTimes,
                         uint64_t numberOfOrigins)
    : SinkMedium(numOfProducers, queryId, querySubPlanId, numberOfOrigins),
      uniqueNetworkSinkDescriptorId(uniqueNetworkSinkDescriptorId), networkManager(TheNetworkManager),
      receiverLocation(destination), bufferManager(TheBufferManager), nesPartition(nesPartition),
      schemaSizeInBytes(outputSchemaSizeInBytes), numOfProducers(numOfProducers), waitTime(waitTime), retryTimes(retryTimes) {
    NES_ASSERT(this->networkManager, "Invalid network manager");
    NES_DEBUG("NetworkSink: Created NetworkSink for partition {} location {}", nesPartition, destination.createZmqURI());
}

SinkMediumTypes NetworkSink::getSinkMediumType() { return SinkMediumTypes::NETWORK_SINK; }

bool NetworkSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
    //if a mobile node is in the process of reconnecting, do not attempt to send data but buffer it instead
    NES_TRACE("context {} writing data", workerContext.getId());
    if (auto* channel = workerContext.getNetworkChannel(nesPartition.getOperatorId())) {
        return channel->sendBuffer(inputBuffer, schemaSizeInBytes, messageSequenceNumber++);
    }
    NES_ASSERT2_FMT(false, "invalid channel on " << nesPartition);
}

void NetworkSink::preSetup() {
    NES_DEBUG("NetworkSink: method preSetup() called {} qep {}", nesPartition.toString(), querySubPlanId);
    NES_ASSERT2_FMT(networkManager->registerSubpartitionEventConsumer(receiverLocation, nesPartition, this),
                    "Cannot register event listener " << nesPartition.toString());
}

void NetworkSink::setup() {
    NES_DEBUG("NetworkSink: method setup() called {} qep {}", nesPartition.toString(), querySubPlanId);
    auto reconf = Runtime::ReconfigurationMessage(queryId,
                                                  querySubPlanId,
                                                  Runtime::ReconfigurationType::Initialize,
                                                  nullptr,
                                                  std::make_any<uint32_t>(numOfProducers));
    this->reconfigure(reconf, *TheWorkerContext);
}

void NetworkSink::shutdown() {
    NES_DEBUG("NetworkSink: shutdown() called {} queryId {} qepsubplan {}", nesPartition.toString(), queryId, querySubPlanId);
    auto reconf = Runtime::ReconfigurationMessage(queryId,
                                                  querySubPlanId,
                                                  Runtime::ReconfigurationType::HardEndOfStream,
                                                  nullptr,
                                                  std::make_any<uint32_t>(numOfProducers));
    this->reconfigure(reconf, *TheWorkerContext);
}

std::string NetworkSink::toString() const { return "NetworkSink: " + nesPartition.toString(); }

void NetworkSink::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& workerContext) {
    NES_DEBUG("NetworkSink: reconfigure() called {} qep {}", nesPartition.toString(), querySubPlanId);
    inherited0::reconfigure(task, workerContext);
    Runtime::QueryTerminationType terminationType = Runtime::QueryTerminationType::Invalid;
    switch (task.getType()) {
        case Runtime::ReconfigurationType::Initialize: {
            auto channel =
                networkManager->registerSubpartitionProducer(receiverLocation, nesPartition, bufferManager, waitTime, retryTimes);
            NES_ASSERT(channel, "Channel not valid partition " << nesPartition);
            workerContext.storeNetworkChannel(nesPartition.getOperatorId(), std::move(channel));
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
        default: {
            break;
        }
    }
    if (terminationType != Runtime::QueryTerminationType::Invalid) {
        //todo #3013: make sure buffers are kept if the device is currently buffering
        if (workerContext.decreaseObjectRefCnt(this) == 1) {
            networkManager->unregisterSubpartitionProducer(nesPartition);
            NES_ASSERT2_FMT(workerContext.releaseNetworkChannel(nesPartition.getOperatorId(),
                                                                terminationType,
                                                                numOfProducers,
                                                                messageSequenceNumber),
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

void NetworkSink::onEvent(Runtime::EventPtr event) {
    NES_DEBUG("NetworkSink::onEvent(event) called. uniqueNetworkSinkDescriptorId: {}", this->uniqueNetworkSinkDescriptorId);
    if (event->getEventType() == Runtime::EventType::kStartSourceEvent) {
        // todo jm continue here. how to obtain local worker context?
    }
}

void NetworkSink::onEvent(Runtime::EventPtr event, Runtime::WorkerContextRef) {
    NES_DEBUG("NetworkSink::onEvent(event, wrkContext) called. uniqueNetworkSinkDescriptorId: {}",
              this->uniqueNetworkSinkDescriptorId);
    // this function currently has no usage
    onEvent(event);
}

OperatorId NetworkSink::getUniqueNetworkSinkDescriptorId() { return uniqueNetworkSinkDescriptorId; }

}// namespace NES::Network
