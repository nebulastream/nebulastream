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
#include <Runtime/ReconfigurationMessage.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>

namespace NES::Network {

NetworkSink::NetworkSink(const SchemaPtr& schema,
                         uint64_t uniqueNetworkSinkDescriptorId,
                         SharedQueryId SharedQueryId,
                         DecomposedQueryPlanId DecomposedQueryPlanId,
                         const NodeLocation& destination,
                         NesPartition nesPartition,
                         Runtime::BufferManagerPtr bufferManager,
                         Runtime::WorkerContextPtr workerContext,
                         NetworkManagerPtr networkManager,
                         size_t numOfProducers,
                         std::chrono::milliseconds waitTime,
                         uint8_t retryTimes,
                         uint64_t numberOfOrigins)
    : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager),
                 numOfProducers,
                 SharedQueryId,
                 DecomposedQueryPlanId,
                 numberOfOrigins),
      uniqueNetworkSinkDescriptorId(uniqueNetworkSinkDescriptorId), networkManager(networkManager), workerContext(workerContext),
      receiverLocation(destination), bufferManager(std::move(bufferManager)), nesPartition(nesPartition),
      numOfProducers(numOfProducers), waitTime(waitTime), retryTimes(retryTimes), reconnectBuffering(false) {
    NES_ASSERT(this->networkManager, "Invalid network manager");
    NES_DEBUG("NetworkSink: Created NetworkSink for partition {} location {}", nesPartition, destination.createZmqURI());
}

SinkMediumTypes NetworkSink::getSinkMediumType() { return SinkMediumTypes::NETWORK_SINK; }

bool NetworkSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
    //if a mobile node is in the process of reconnecting, do not attempt to send data but buffer it instead
    NES_TRACE("context {} writing data", workerContext.getId());
    if (reconnectBuffering) {
        NES_TRACE("context {} buffering data", workerContext.getId());
        workerContext.insertIntoStorage(this->nesPartition, inputBuffer);
        return true;
    }

    auto* channel = workerContext.getNetworkChannel(nesPartition.getOperatorId());
    if (channel) {
        auto success = channel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes(), messageSequence++);
        if (success) {
            insertIntoStorageCallback(inputBuffer, workerContext);
        }
        return success;
    }
    NES_ASSERT2_FMT(false, "invalid channel on " << nesPartition);
    return false;
}

void NetworkSink::preSetup() {
    NES_DEBUG("NetworkSink: method preSetup() called {} qep {}", nesPartition.toString(), decomposedQueryPlanId);
    NES_ASSERT2_FMT(
        networkManager->registerSubpartitionEventConsumer(receiverLocation, nesPartition, inherited1::shared_from_this()),
        "Cannot register event listener " << nesPartition.toString());
}

void NetworkSink::setup() {
    NES_DEBUG("NetworkSink: method setup() called {} qep {}", nesPartition.toString(), decomposedQueryPlanId);
    auto reconf = Runtime::ReconfigurationMessage(sharedQueryId,
                                                  decomposedQueryPlanId,
                                                  Runtime::ReconfigurationType::Initialize,
                                                  inherited0::shared_from_this(),
                                                  std::make_any<uint32_t>(numOfProducers));
    this->reconfigure(reconf, *workerContext);
}

void NetworkSink::shutdown() {
    NES_DEBUG("NetworkSink: shutdown() called {} SharedQueryId {} qepsubplan {}",
              nesPartition.toString(),
              sharedQueryId,
              decomposedQueryPlanId);
    networkManager->unregisterSubpartitionProducer(nesPartition);
}

std::string NetworkSink::toString() const { return "NetworkSink: " + nesPartition.toString(); }

void NetworkSink::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& workerContext) {
    NES_DEBUG("NetworkSink: reconfigure() called {} qep {}", nesPartition.toString(), decomposedQueryPlanId);
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
        case Runtime::ReconfigurationType::Destroy:
        case Runtime::ReconfigurationType::ConnectToNewReceiver:
        case Runtime::ReconfigurationType::ConnectionEstablished:
        case Runtime::ReconfigurationType::UpdateVersion: NES_NOT_IMPLEMENTED();
    }
    if (terminationType != Runtime::QueryTerminationType::Invalid) {
        //todo #3013: make sure buffers are kept if the device is currently buffering
        if (workerContext.decreaseObjectRefCnt(this) == 1) {
            networkManager->unregisterSubpartitionProducer(nesPartition);
            NES_ASSERT2_FMT(
                workerContext.releaseNetworkChannel(nesPartition.getOperatorId(), terminationType, 1, messageSequence),
                "Cannot remove network channel " << nesPartition.toString());
            NES_DEBUG("NetworkSink: reconfigure() released channel on {} Thread {}",
                      nesPartition.toString(),
                      Runtime::NesThread::getId());
        }
    }
}

void NetworkSink::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    NES_DEBUG("NetworkSink: postReconfigurationCallback() called {} parent plan {}",
              nesPartition.toString(),
              decomposedQueryPlanId);
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
