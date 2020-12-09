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

#include <Network/NesPartition.hpp>
#include <Network/NetworkSource.hpp>
#include <Util/Logger.hpp>

namespace NES {
namespace Network {

NetworkSource::NetworkSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                             NetworkManagerPtr networkManager, NesPartition nesPartition)
    : DataSource(schema, bufferManager, queryManager, nesPartition.getOperatorId()), networkManager(networkManager),
      nesPartition(nesPartition) {
    NES_INFO("NetworkSource: Initializing NetworkSource for " << nesPartition.toString());
    NES_ASSERT(this->networkManager, "Invalid network manager");
}

NetworkSource::~NetworkSource() { NES_DEBUG("NetworkSink: Destroying NetworkSource " << nesPartition.toString()); }

std::optional<TupleBuffer> NetworkSource::receiveData() {
    NES_THROW_RUNTIME_ERROR("NetworkSource: ReceiveData() called, but method is invalid and should not be used.");
}

SourceType NetworkSource::getType() const { return NETWORK_SOURCE; }

const std::string NetworkSource::toString() const { return "NetworkSource: " + nesPartition.toString(); }

bool NetworkSource::start() {
    NES_DEBUG("NetworkSource: start called on " << nesPartition);
    return networkManager->registerSubpartitionConsumer(nesPartition) == 0;
}

bool NetworkSource::stop() {
    // TODO ensure proper termination: what should happen here when we call stop but refCnt has not reached zero?
    NES_DEBUG("NetworkSource: stop called on " << nesPartition);
    networkManager->unregisterSubpartitionConsumer(nesPartition);
    return true;
}

void NetworkSource::runningRoutine(NodeEngine::BufferManagerPtr, NodeEngine::QueryManagerPtr) {
    NES_THROW_RUNTIME_ERROR("NetworkSource: runningRoutine() called, but method is invalid and should not be used.");
}

}// namespace Network
}// namespace NES