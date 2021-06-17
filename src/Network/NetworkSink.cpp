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
#include <Network/OutputChannelKey.hpp>
#include <NodeEngine/StopQueryMessage.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <utility>

namespace NES {

namespace Network {

NetworkSink::NetworkSink(SchemaPtr schema,
                         QuerySubPlanId parentPlanId,
                         NetworkManagerPtr networkManager,
                         const NodeLocation nodeLocation,
                         NesPartition nesPartition,
                         NodeEngine::BufferManagerPtr bufferManager,
                         NodeEngine::QueryManagerPtr queryManager,
                         std::chrono::seconds waitTime,
                         uint8_t retryTimes)
    : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), parentPlanId), networkManager(std::move(networkManager)),
      nodeLocation(nodeLocation), nesPartition(nesPartition), queryManager(queryManager), waitTime(waitTime),
      retryTimes(retryTimes), outputChannelKey(OutputChannelKey(parentPlanId, nesPartition.getOperatorId())) {
    NES_ASSERT(this->networkManager, "Invalid network manager");
    NES_DEBUG("NetworkSink: Created NetworkSink for partition " << nesPartition << " location " << nodeLocation.createZmqURI());
}

SinkMediumTypes NetworkSink::getSinkMediumType() { return NETWORK_SINK; }

NetworkSink::~NetworkSink() { NES_INFO("NetworkSink: Destructor called " << nesPartition); }

bool NetworkSink::writeData(NodeEngine::TupleBuffer& inputBuffer, NodeEngine::WorkerContext& workerContext) {
    auto* channel = workerContext.getChannel(outputChannelKey);
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
        NodeEngine::ReconfigurationMessage(parentPlanId, NodeEngine::Initialize, shared_from_this()),
        false);
}

void NetworkSink::shutdown() {
    NES_DEBUG("NetworkSink: shutdown() called " << nesPartition.toString() << " qep " << parentPlanId);
}

const std::string NetworkSink::toString() const { return "NetworkSink: " + nesPartition.toString(); }

void NetworkSink::reconfigure(NodeEngine::ReconfigurationMessage& task, NodeEngine::WorkerContext& workerContext) {
    NES_DEBUG("NetworkSink: reconfigure() called " << nesPartition.toString() << " parent plan " << parentPlanId);
    NES::SinkMedium::reconfigure(task, workerContext);
    switch (task.getType()) {
        case NodeEngine::Initialize: {
            auto channel = networkManager->registerSubpartitionProducer(nodeLocation, nesPartition, waitTime, retryTimes);
            NES_ASSERT(channel, "Channel not valid partition " << nesPartition);
            workerContext.storeChannel(outputChannelKey, std::move(channel));
            NES_DEBUG("NetworkSink: reconfigure() stored channel on " << nesPartition.toString() << " Thread "
                                                                      << NodeEngine::NesThread::getId());
            break;
        }
        case NodeEngine::Destroy: {
            auto notifyRelease = task.getUserData<bool>();
            workerContext.releaseChannel(outputChannelKey, notifyRelease);
            NES_DEBUG("NetworkSink: reconfigure() released channel on " << nesPartition.toString() << " Thread "
                                                                        << NodeEngine::NesThread::getId());
            break;
        }
        case NodeEngine::QueryReconfiguration: {
            auto queryReconfigurationPlan = task.getUserData<QueryReconfigurationPlanPtr>();
            auto* channel = workerContext.getChannel(outputChannelKey);
            channel->sendReconfigurationMessage(queryReconfigurationPlan);
            break;
        }
        default: {
            break;
        }
    }
}

void NetworkSink::postReconfigurationCallback(NodeEngine::ReconfigurationMessage& task) {
    NES_DEBUG("NetworkSink: postReconfigurationCallback() called " << nesPartition.toString() << " parent plan " << parentPlanId);
    NES::SinkMedium::postReconfigurationCallback(task);
    switch (task.getType()) {
        case NodeEngine::HardEndOfStream:
        case NodeEngine::SoftEndOfStream: {
            auto triggerEoSMsg = std::make_any<bool>(true);
            auto destroyMsg = NodeEngine::ReconfigurationMessage(parentPlanId,
                                                                 NodeEngine::Destroy,
                                                                 shared_from_this(),
                                                                 std::move(triggerEoSMsg));
            queryManager->addReconfigurationMessage(parentPlanId, destroyMsg, false);
            break;
        }
        case NodeEngine::StopViaReconfiguration: {
            auto stopMessage = task.getUserData<NodeEngine::StopQueryMessagePtr>();
            auto reconfigurationMsg = std::make_any<QueryReconfigurationPlanPtr>(stopMessage->getQueryReconfigurationPlan());

            auto reconfigurationMessage = NodeEngine::ReconfigurationMessage(parentPlanId,
                                                                             NodeEngine::QueryReconfiguration,
                                                                             shared_from_this(),
                                                                             std::move(reconfigurationMsg));
            queryManager->addReconfigurationMessage(parentPlanId, reconfigurationMessage, false);
            auto triggerEoSMsg = std::make_any<bool>(false);
            auto destroyMsg = NodeEngine::ReconfigurationMessage(parentPlanId,
                                                                 NodeEngine::Destroy,
                                                                 shared_from_this(),
                                                                 std::move(triggerEoSMsg));
            queryManager->addReconfigurationMessage(parentPlanId, destroyMsg, false);
            break;
        }
        default: {
            break;
        }
    }
}
}// namespace Network
}// namespace NES
