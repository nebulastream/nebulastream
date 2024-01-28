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
#include <Runtime/BufferStorage.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
uint32_t WorkerContext::getId() const { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

uint32_t WorkerContext::getQueueId() const { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

void WorkerContext::setObjectRefCnt(void* object, uint32_t refCnt) { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

uint32_t WorkerContext::increaseObjectRefCnt(void* object) { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

uint32_t WorkerContext::decreaseObjectRefCnt(void* object) { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

TupleBuffer WorkerContext::allocateTupleBuffer() { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

void WorkerContext::storeNetworkChannel(NES::OperatorId id, Network::NetworkChannelPtr&& channel) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void WorkerContext::createStorage(Network::NesPartition nesPartition) { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

void WorkerContext::insertIntoStorage(Network::NesPartition nesPartition, NES::Runtime::TupleBuffer buffer) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void WorkerContext::trimStorage(Network::NesPartition nesPartition, uint64_t timestamp) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

std::optional<NES::Runtime::TupleBuffer> WorkerContext::getTopTupleFromStorage(Network::NesPartition nesPartition) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void WorkerContext::removeTopTupleFromStorage(Network::NesPartition nesPartition) { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

bool WorkerContext::releaseNetworkChannel(NES::OperatorId id, Runtime::QueryTerminationType terminationType) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

void WorkerContext::storeEventOnlyChannel(NES::OperatorId id, Network::EventOnlyNetworkChannelPtr&& channel) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

bool WorkerContext::releaseEventOnlyChannel(NES::OperatorId id, Runtime::QueryTerminationType terminationType) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

Network::NetworkChannel* WorkerContext::getNetworkChannel(NES::OperatorId ownerId) { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

Network::EventOnlyNetworkChannel* WorkerContext::getEventOnlyNetworkChannel(NES::OperatorId ownerId) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

LocalBufferPool* WorkerContext::getBufferProviderTLS() { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

LocalBufferPoolPtr WorkerContext::getBufferProvider() { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

#pragma clang diagnostic pop
}// namespace NES::Runtime