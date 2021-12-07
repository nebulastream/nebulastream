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
#include <Network/NetworkChannel.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSource.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::Network {

NetworkSource::NetworkSource(SchemaPtr schema,
                             Runtime::BufferManagerPtr bufferManager,
                             Runtime::QueryManagerPtr queryManager,
                             NetworkManagerPtr networkManager,
                             NesPartition nesPartition,
                             NodeLocation sinkLocation,
                             size_t numSourceLocalBuffers,
                             std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors,
                             std::chrono::seconds waitTime,
                             uint8_t retryTimes)
    : DataSource(std::move(schema),
                 std::move(bufferManager),
                 std::move(queryManager),
                 nesPartition.getOperatorId(),
                 numSourceLocalBuffers,
                 DataSource::FREQUENCY_MODE,
                 std::move(successors)),
      networkManager(std::move(networkManager)), nesPartition(nesPartition), sinkLocation(std::move(sinkLocation)),
      waitTime(waitTime), retryTimes(retryTimes) {
    NES_INFO("NetworkSource: Initializing NetworkSource for " << nesPartition.toString());
    NES_ASSERT(this->networkManager, "Invalid network manager");
}

std::optional<Runtime::TupleBuffer> NetworkSource::receiveData() {
    NES_THROW_RUNTIME_ERROR("NetworkSource: ReceiveData() called, but method is invalid and should not be used.");
}

SourceType NetworkSource::getType() const { return NETWORK_SOURCE; }

std::string NetworkSource::toString() const { return "NetworkSource: " + nesPartition.toString(); }

// this is necessary to use std::visit below (see example: https://en.cppreference.com/w/cpp/utility/variant/visit)
namespace detail {
template<class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
template<class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;
}// namespace detail

bool NetworkSource::start() {
    using namespace Runtime;
    NES_DEBUG("NetworkSource: start called on " << nesPartition);
    auto emitter = shared_from_base<DataEmitter>();
    if (networkManager->registerSubpartitionConsumer(nesPartition, sinkLocation, emitter)) {
        for (const auto& successor : executableSuccessors) {
            auto querySubPlanId = std::visit(detail::overloaded{[](DataSinkPtr sink) {
                                                                    return sink->getParentPlanId();
                                                                },
                                                                [](Execution::ExecutablePipelinePtr pipeline) {
                                                                    return pipeline->getQuerySubPlanId();
                                                                }},
                                             successor);
            auto newReconf = ReconfigurationMessage(querySubPlanId, Runtime::Initialize, shared_from_base<DataSource>());
            queryManager->addReconfigurationMessage(querySubPlanId, newReconf, false);
        }
        return true;
    }
    return false;
}

bool NetworkSource::stop(bool) {
    NES_DEBUG("NetworkSource: stop called on " << nesPartition << " is ignored");
    return true;
}

void NetworkSource::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& workerContext) {
    NES_DEBUG("NetworkSource: reconfigure() called " << nesPartition.toString());
    NES::DataSource::reconfigure(task, workerContext);
    switch (task.getType()) {
        case Runtime::Initialize: {
            // we need to check again because between the invocations of
            // NetworkSource::start() and NetworkSource::reconfigure() the query might have
            // been stopped for some reasons
            if (networkManager->isPartitionConsumerRegistered(nesPartition) == PartitionRegistrationStatus::Deleted) {
                return;
            }
            auto channel = networkManager->registerSubpartitionEventProducer(sinkLocation,
                                                                             nesPartition,
                                                                             localBufferManager,
                                                                             waitTime,
                                                                             retryTimes);
            if (channel == nullptr) {
                NES_DEBUG("NetworkSource: reconfigure() cannot get event channel " << nesPartition.toString() << " on Thread "
                                                                                   << Runtime::NesThread::getId());
                return;// partition was deleted on the other side of the channel.. no point in waiting for a channel
            }
            workerContext.storeEventOnlyChannel(nesPartition.getOperatorId(), std::move(channel));
            NES_DEBUG("NetworkSource: reconfigure() stored event-channel " << nesPartition.toString() << " Thread "
                                                                           << Runtime::NesThread::getId());
            break;
        }
        case Runtime::Destroy:
        case Runtime::HardEndOfStream:
        case Runtime::SoftEndOfStream: {
            workerContext.releaseEventOnlyChannel(nesPartition.getOperatorId());
            NES_DEBUG("NetworkSource: reconfigure() released channel on " << nesPartition.toString() << " Thread "
                                                                          << Runtime::NesThread::getId());
            break;
        }
        default: {
            break;
        }
    }
}

void NetworkSource::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    NES_DEBUG("NetworkSource: postReconfigurationCallback() called " << nesPartition.toString());
    NES::DataSource::postReconfigurationCallback(task);
    switch (task.getType()) {
        case Runtime::Destroy:
        case Runtime::HardEndOfStream:
        case Runtime::SoftEndOfStream: {
            NES_DEBUG("NetworkSource: postReconfigurationCallback(): unregistering SubpartitionConsumer "
                      << nesPartition.toString());
            networkManager->unregisterSubpartitionConsumer(nesPartition);
            return;
        }
        default: {
            break;
        }
    }
}

void NetworkSource::runningRoutine(const Runtime::BufferManagerPtr&, const Runtime::QueryManagerPtr&) {
    NES_THROW_RUNTIME_ERROR("NetworkSource: runningRoutine() called, but method is invalid and should not be used.");
}

}// namespace NES::Network