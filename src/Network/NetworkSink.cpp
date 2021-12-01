/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <utility>

namespace NES::Network {

NetworkSink::NetworkSink(const SchemaPtr& schema,
                         QuerySubPlanId querySubPlanId,
                         NetworkManagerPtr networkManager,
                         const NodeLocation& destination,
                         NesPartition nesPartition,
                         const Runtime::BufferManagerPtr& bufferManager,
                         Runtime::QueryManagerPtr queryManager,
                         Runtime::BufferStoragePtr bufferStorage,
                         size_t numOfProducers,
                         std::chrono::seconds waitTime,
                         uint8_t retryTimes)
    : inherited0(std::make_shared<NesFormat>(schema, bufferManager), queryManager, querySubPlanId),
      networkManager(std::move(networkManager)), queryManager(queryManager), receiverLocation(destination),
      bufferManager(std::move(bufferManager)), bufferStorage(std::move(bufferStorage)), nesPartition(nesPartition),
      numOfProducers(numOfProducers), waitTime(waitTime), retryTimes(retryTimes) {
    NES_ASSERT(this->networkManager, "Invalid network manager");
    NES_DEBUG("NetworkSink: Created NetworkSink for partition " << nesPartition << " location " << destination.createZmqURI());
}

SinkMediumTypes NetworkSink::getSinkMediumType() { return NETWORK_SINK; }

bool NetworkSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
    auto* channel = workerContext.getNetworkChannel(nesPartition.getOperatorId());
    //bufferStorage->insertBuffer(inputBuffer.getSequenceNumber() + inputBuffer.getOriginId(), inputBuffer);
    if (channel) {
        return channel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes());
    }
    NES_ASSERT2_FMT(false, "invalid channel on " << nesPartition);
    return false;
}

void NetworkSink::setup() {
    NES_DEBUG("NetworkSink: method setup() called " << nesPartition.toString() << " qep " << querySubPlanId);
    networkManager->registerSubpartitionEventConsumer(receiverLocation, nesPartition, inherited1::shared_from_this());
    auto reconf = Runtime::ReconfigurationMessage(querySubPlanId,
                                                  Runtime::Initialize,
                                                  inherited0::shared_from_this(),
                                                  std::make_any<uint32_t>(numOfProducers));
    queryManager->addReconfigurationMessage(querySubPlanId, reconf, false);
}

void NetworkSink::shutdown() {
    NES_DEBUG("NetworkSink: shutdown() called " << nesPartition.toString() << " qep " << querySubPlanId);
}

std::string NetworkSink::toString() const { return "NetworkSink: " + nesPartition.toString(); }

void NetworkSink::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& workerContext) {
    NES_DEBUG("NetworkSink: reconfigure() called " << nesPartition.toString() << " parent plan " << querySubPlanId);
    inherited0::reconfigure(task, workerContext);
    switch (task.getType()) {
        case Runtime::Initialize: {
            auto channel =
                networkManager->registerSubpartitionProducer(receiverLocation, nesPartition, bufferManager, waitTime, retryTimes);
            NES_ASSERT(channel, "Channel not valid partition " << nesPartition);
            workerContext.storeNetworkChannel(nesPartition.getOperatorId(), std::move(channel));
            workerContext.setObjectRefCnt(this, task.getUserData<uint32_t>());
            NES_DEBUG("NetworkSink: reconfigure() stored channel on " << nesPartition.toString() << " Thread "
                                                                      << Runtime::NesThread::getId() << " ref cnt "
                                                                      << task.getUserData<uint32_t>());
            break;
        }
        case Runtime::HardEndOfStream:
        case Runtime::SoftEndOfStream: {
            if (workerContext.decreaseObjectRefCnt(this) == 1) {
                networkManager->unregisterSubpartitionProducer(nesPartition);
                NES_ASSERT2_FMT(workerContext.releaseNetworkChannel(nesPartition.getOperatorId()),
                                "Cannot remove network channel " << nesPartition.toString());
                NES_DEBUG("NetworkSink: reconfigure() released channel on " << nesPartition.toString() << " Thread "
                                                                            << Runtime::NesThread::getId());
            }
            break;
        }
        default: {
            break;
        }
    }
}

void NetworkSink::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    NES_DEBUG("NetworkSink: postReconfigurationCallback() called " << nesPartition.toString() << " parent plan "
                                                                   << querySubPlanId);
    inherited0::postReconfigurationCallback(task);
    switch (task.getType()) {
        case Runtime::HardEndOfStream:
        case Runtime::SoftEndOfStream: {
            networkManager->unregisterSubpartitionProducer(nesPartition);
            break;
        }
        default: {
            break;
        }
    }
}

void NetworkSink::onEvent(Runtime::BaseEvent& event) {
    auto qep = queryManager->getQueryExecutionPlan(querySubPlanId);
    qep->onEvent(event);
}

}// namespace NES::Network
