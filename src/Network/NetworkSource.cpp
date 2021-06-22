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
#include <utility>

namespace NES::Network {

NetworkSource::NetworkSource(SchemaPtr schema,
                             Runtime::BufferManagerPtr bufferManager,
                             Runtime::QueryManagerPtr queryManager,
                             NetworkManagerPtr networkManager,
                             NesPartition nesPartition,
                             OperatorId logicalSourceOperatorId,
                             size_t numSourceLocalBuffers,
                             std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(std::move(schema),
                 std::move(bufferManager),
                 std::move(queryManager),
                 nesPartition.getOperatorId(),
                 logicalSourceOperatorId,
                 numSourceLocalBuffers,
                 DataSource::FREQUENCY_MODE,
                 std::move(successors)),
      networkManager(std::move(networkManager)), nesPartition(nesPartition) {
    NES_INFO("NetworkSource: Initializing NetworkSource for " << nesPartition.toString());
    NES_ASSERT(this->networkManager, "Invalid network manager");
}

NetworkSource::~NetworkSource() { NES_DEBUG("NetworkSink: Destroying NetworkSource " << nesPartition.toString()); }

std::optional<Runtime::TupleBuffer> NetworkSource::receiveData() {
    NES_THROW_RUNTIME_ERROR("NetworkSource: ReceiveData() called, but method is invalid and should not be used.");
}

SourceType NetworkSource::getType() const { return NETWORK_SOURCE; }

std::string NetworkSource::toString() const { return "NetworkSource: " + nesPartition.toString(); }

bool NetworkSource::start() {
    NES_DEBUG("NetworkSource: start called on " << nesPartition);
    auto emitter = shared_from_base<DataEmitter>();
    return networkManager->registerSubpartitionConsumer(nesPartition, emitter);
}

bool NetworkSource::stop(bool) {
    NES_DEBUG("NetworkSource: stop called on " << nesPartition);
    return true;
}

NesPartition NetworkSource::getNesPartition() { return nesPartition; }

void NetworkSource::runningRoutine(const Runtime::BufferManagerPtr&, const Runtime::QueryManagerPtr&) {
    NES_THROW_RUNTIME_ERROR("NetworkSource: runningRoutine() called, but method is invalid and should not be used.");
}

void NetworkSource::emitWork(Runtime::TupleBuffer& buffer) { DataSource::emitWork(buffer); }

}// namespace NES::Network