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

#include <Network/NetworkSink.hpp>
#include <Network/NodeLocationPOD.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <utility>

namespace NES::Network {

NetworkSink::NetworkSink(const SchemaPtr& schema,
                         OperatorId operatorId,
                         QuerySubPlanId parentPlanId,
                         NetworkManagerPtr networkManager,
                         const NodeLocation& nodeLocation,
                         NesPartition nesPartition,
                         const Runtime::BufferManagerPtr& bufferManager,
                         Runtime::QueryManagerPtr queryManager,
                         Runtime::BufferStoragePtr bufferStorage,
                         std::chrono::seconds waitTime,
                         uint8_t retryTimes)
    : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), parentPlanId), operatorId (operatorId) ,networkManager(std::move(networkManager)),
      queryManager(std::move(queryManager)), bufferStorage(std::move(bufferStorage)), nodeLocation(nodeLocation),
      nesPartition(nesPartition), waitTime(waitTime), retryTimes(retryTimes) {
    NES_ASSERT(this->networkManager, "Invalid network manager");
    NES_DEBUG("NetworkSink: Created NetworkSink for partition " << nesPartition << " location " << nodeLocation.createZmqURI());
}

SinkMediumTypes NetworkSink::getSinkMediumType() { return NETWORK_SINK; }

NetworkSink::~NetworkSink() { NES_INFO("NetworkSink: Destructor called " << nesPartition); }

bool NetworkSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) {
    auto* channel = workerContext.getChannel(nesPartition.getOperatorId());
    //bufferStorage->insertBuffer(inputBuffer.getSequenceNumber() + inputBuffer.getOriginId(), inputBuffer);
    if (channel) {
        return channel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes());
    }
    NES_ASSERT2_FMT(false, "invalid channel on " << nesPartition);
    return false;
}

void NetworkSink::setup() {
    NES_DEBUG("NetworkSink: method setup() called " << nesPartition.toString() << " qep " << parentPlanId);
    queryManager->addReconfigurationMessage(
        parentPlanId,
        Runtime::ReconfigurationMessage(parentPlanId, Runtime::Initialize, shared_from_this()),
        false);
}

void NetworkSink::shutdown() {
    NES_DEBUG("NetworkSink: shutdown() called " << nesPartition.toString() << " qep " << parentPlanId);
}

std::string NetworkSink::toString() const { return "NetworkSink: " + nesPartition.toString(); }

void NetworkSink::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& workerContext) {
    NES_DEBUG("NetworkSink: reconfigure() called " << nesPartition.toString() << " parent plan " << parentPlanId);
    NES::SinkMedium::reconfigure(task, workerContext);
    switch (task.getType()) {
        case Runtime::Initialize: {
            auto channel = networkManager->registerSubpartitionProducer(nodeLocation, nesPartition, waitTime, retryTimes);
            NES_ASSERT(channel, "Channel not valid partition " << nesPartition);
            workerContext.storeChannel(nesPartition.getOperatorId(), std::move(channel));
            NES_DEBUG("NetworkSink: reconfigure() stored channel on " << nesPartition.toString() << " Thread "
                                                                      << Runtime::NesThread::getId());
            break;
        }
        case Runtime::Destroy: {
            workerContext.releaseChannel(nesPartition.getOperatorId());
            NES_DEBUG("NetworkSink: reconfigure() released channel on " << nesPartition.toString() << " Thread "
                                                                        << Runtime::NesThread::getId());
            break;
        }
        case NodeEngine::UpdateSinks: {

                NodeLocationPOD pod = task.getUserData<NodeLocationPOD>();
                Network::NodeLocation updatedNodeLocation(pod.nodeId, pod.hostname, pod.port);

                auto updatedChannel =
                        networkManager->registerSubpartitionProducer(updatedNodeLocation, nesPartition, waitTime, retryTimes);
                workerContext.updateChannel(nesPartition.getOperatorId(), std::move(updatedChannel));
                break;
        }
        default: {
            break;
        }
    }
}

void NetworkSink::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    NES_DEBUG("NetworkSink: postReconfigurationCallback() called " << nesPartition.toString() << " parent plan " << parentPlanId);
    NES::SinkMedium::postReconfigurationCallback(task);
    switch (task.getType()) {
        case Runtime::HardEndOfStream:
        case Runtime::SoftEndOfStream: {
            auto newReconf = Runtime::ReconfigurationMessage(parentPlanId, Runtime::Destroy, shared_from_this());
            queryManager->addReconfigurationMessage(parentPlanId, newReconf, false);
            break;
        }
        case NodeEngine::UpdateSinks:{
            NodeLocationPOD pod = task.getUserData<NodeLocationPOD>();
            Network::NodeLocation updatedNodeLocation(pod.nodeId, pod.hostname, pod.port);
            nodeLocation = updatedNodeLocation;
            //queryManager->getrunningQEPs()[]

            break;

        }
        default: {
            break;
        }
    }
}
NodeLocation NetworkSink::getNodeLocation() {
    return nodeLocation; }
OperatorId NetworkSink::getOperatorId() {
    return operatorId;
}
}// namespace NES::Network
