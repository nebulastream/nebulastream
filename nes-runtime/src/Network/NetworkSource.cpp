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
#include <Network/NetworkSource.hpp>
#include <Operators/LogicalOperators/Network/NesPartition.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Reconfiguration/Metadata/DrainQueryMetadata.hpp>
#include <Reconfiguration/Metadata/UpdateAndDrainQueryMetadata.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/ReconfigurationMessage.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <utility>

namespace NES::Network {

NetworkSource::NetworkSource(SchemaPtr schema,
                             Runtime::BufferManagerPtr bufferManager,
                             Runtime::QueryManagerPtr queryManager,
                             NetworkManagerPtr networkManager,
                             NesPartition nesPartition,
                             NodeLocation sinkLocation,
                             size_t numSourceLocalBuffers,
                             std::chrono::milliseconds waitTime,
                             uint8_t retryTimes,
                             std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors,
                             DecomposedQueryPlanVersion version,
                             OperatorId uniqueNetworkSourceIdentifier,
                             const std::string& physicalSourceName)

    : DataSource(std::move(schema),
                 std::move(bufferManager),
                 std::move(queryManager),
                 nesPartition.getOperatorId(),
                 /*invalid origin id for the network source, as the network source does not change the origin id*/
                 INVALID_ORIGIN_ID,
                 /*invalid statistic id for the network source, as the network source does not change the statistic id*/
                 INVALID_STATISTIC_ID,
                 numSourceLocalBuffers,
                 GatheringMode::INTERVAL_MODE,
                 physicalSourceName,
                 false,
                 false,
                 std::move(successors)),
      networkManager(std::move(networkManager)), nesPartition(nesPartition), sinkLocation(std::move(sinkLocation)),
      waitTime(waitTime), retryTimes(retryTimes), version(version), uniqueNetworkSourceIdentifier(uniqueNetworkSourceIdentifier) {
    NES_ASSERT(this->networkManager, "Invalid network manager");
}

std::optional<Runtime::TupleBuffer> NetworkSource::receiveData() {
    NES_THROW_RUNTIME_ERROR("NetworkSource: ReceiveData() called, but method is invalid and should not be used.");
}

SourceType NetworkSource::getType() const { return SourceType::NETWORK_SOURCE; }

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

bool NetworkSource::bind() {
    auto emitter = shared_from_base<DataEmitter>();
    return networkManager->registerSubpartitionConsumer(nesPartition, sinkLocation, emitter);
}

bool NetworkSource::start() {
    using namespace Runtime;
    NES_DEBUG("NetworkSource: start called on {}, waitingTime: {}, retryTime: {}", nesPartition, waitTime.count(), retryTimes);
    auto emitter = shared_from_base<DataEmitter>();
    auto expected = false;
    if (running.compare_exchange_strong(expected, true)) {
        for (const auto& successor : executableSuccessors) {
            auto decomposedQueryId = std::visit(detail::overloaded{[](DataSinkPtr sink) {
                                                                       return sink->getParentPlanId();
                                                                   },
                                                                   [](Execution::ExecutablePipelinePtr pipeline) {
                                                                       return pipeline->getDecomposedQueryId();
                                                                   }},
                                                successor);
            auto sharedQueryId = std::visit(detail::overloaded{[](DataSinkPtr sink) {
                                                                   return sink->getSharedQueryId();
                                                               },
                                                               [](Execution::ExecutablePipelinePtr pipeline) {
                                                                   return pipeline->getSharedQueryId();
                                                               }},
                                            successor);

            auto newReconf = ReconfigurationMessage(sharedQueryId,
                                                    decomposedQueryId,
                                                    version,
                                                    Runtime::ReconfigurationType::Initialize,
                                                    shared_from_base<DataSource>());
            queryManager->addReconfigurationMessage(sharedQueryId, decomposedQueryId, version, newReconf, true);
            break;// hack as currently we assume only one executableSuccessor
        }
        NES_DEBUG("NetworkSource: start completed on {}", nesPartition);
        return true;
    }
    return false;
}

bool NetworkSource::fail() {
    using namespace Runtime;
    bool expected = true;
    if (running.compare_exchange_strong(expected, false)) {
        NES_DEBUG("NetworkSource: fail called on {}", nesPartition);
        auto newReconf = ReconfigurationMessage(INVALID_SHARED_QUERY_ID,
                                                INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                ReconfigurationType::FailEndOfStream,
                                                DataSource::shared_from_base<DataSource>());
        queryManager->addReconfigurationMessage(INVALID_SHARED_QUERY_ID,
                                                INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                newReconf,
                                                false);
        queryManager->notifySourceCompletion(shared_from_base<DataSource>(), Runtime::QueryTerminationType::Failure);
        return queryManager->addEndOfStream(shared_from_base<NetworkSource>(), Runtime::QueryTerminationType::Failure);
    }
    return false;
}

bool NetworkSource::stop(Runtime::QueryTerminationType type) {
    using namespace Runtime;
    bool expected = true;
    NES_ASSERT2_FMT(type == QueryTerminationType::HardStop,
                    "NetworkSource::stop only supports HardStop or Failure :: partition " << nesPartition);
    if (running.compare_exchange_strong(expected, false)) {
        NES_DEBUG("NetworkSource: stop called on {}", nesPartition);
        auto newReconf = ReconfigurationMessage(INVALID_SHARED_QUERY_ID,
                                                INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                ReconfigurationType::HardEndOfStream,
                                                DataSource::shared_from_base<DataSource>());
        queryManager->addReconfigurationMessage(INVALID_SHARED_QUERY_ID,
                                                INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                newReconf,
                                                false);
        queryManager->notifySourceCompletion(shared_from_base<DataSource>(), Runtime::QueryTerminationType::HardStop);
        queryManager->addEndOfStream(shared_from_base<DataSource>(), Runtime::QueryTerminationType::HardStop);
        NES_DEBUG("NetworkSource: stop called on {} sent hard eos", nesPartition);
    } else {
        NES_DEBUG("NetworkSource: stop called on {} but was already stopped", nesPartition);
    }
    return true;
}

void NetworkSource::onEvent(Runtime::BaseEvent&) { NES_DEBUG("NetworkSource: received an event"); }

void NetworkSource::reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& workerContext) {
    NES_DEBUG("NetworkSource: reconfigure() called {} for the type {}",
              nesPartition.toString(),
              magic_enum::enum_name(task.getType()));
    NES::DataSource::reconfigure(task, workerContext);
    bool isTermination = false;
    auto terminationType = Runtime::QueryTerminationType::Failure;

    switch (task.getType()) {
        case Runtime::ReconfigurationType::UpdateVersion: {
            if (!networkManager->getConnectSourceEventChannelsAsync()) {
                NES_THROW_RUNTIME_ERROR(
                    "Attempt to reconfigure a network source but asynchronous connecting of event channels is not "
                    "activated. To use source reconfiguration allow asynchronous connecting in the the configuration");
            }
            workerContext.releaseEventOnlyChannel(uniqueNetworkSourceIdentifier, terminationType);
            NES_DEBUG("NetworkSource: reconfigure() released channel on {} Thread {}",
                      nesPartition.toString(),
                      Runtime::NesThread::getId());
            break;
        }
        case Runtime::ReconfigurationType::Initialize: {
            // we need to check again because between the invocations of
            // NetworkSource::start() and NetworkSource::reconfigure() the query might have
            // been stopped for some reason
            if (networkManager->isPartitionConsumerRegistered(nesPartition) == PartitionRegistrationStatus::Deleted) {
                NES_WARNING(
                    "NetworkManager shows the partition {} to be deleted, but now we should init it here, so we simply return!",
                    nesPartition.toString());
                return;
            }
            // TODO: #5171 uncomment event channels
            /*
            if (networkManager->getConnectSourceEventChannelsAsync()) {
                auto channelFuture = networkManager->registerSubpartitionEventProducerAsync(sinkLocation,
                                                                                            nesPartition,
                                                                                            localBufferManager,
                                                                                            waitTime,
                                                                                            retryTimes);
                workerContext.storeEventChannelFuture(this->operatorId, version, std::move(channelFuture));
                break;
            } else {
                auto channel = networkManager->registerSubpartitionEventProducer(sinkLocation,
                                                                                 nesPartition,
                                                                                 localBufferManager,
                                                                                 waitTime,
                                                                                 retryTimes);
                if (channel == nullptr) {
                    NES_WARNING("NetworkSource: reconfigure() cannot get event channel {} on Thread {}",
                                nesPartition.toString(),
                                Runtime::NesThread::getId());
                    return;// partition was deleted on the other side of the channel... no point in waiting for a channel
                }
                workerContext.storeEventOnlyChannel(this->operatorId, std::move(channel));
                NES_DEBUG("NetworkSource: reconfigure() stored event-channel {} Thread {}",
                          nesPartition.toString(),
                          Runtime::NesThread::getId());
            }*/
            break;
        }
        case Runtime::ReconfigurationType::Destroy: {
            // necessary as event channel are lazily created so in the case of an immediate stop
            // they might not be established yet
            terminationType = Runtime::QueryTerminationType::Graceful;
            isTermination = true;
            break;
        }
        case Runtime::ReconfigurationType::HardEndOfStream: {
            terminationType = Runtime::QueryTerminationType::HardStop;
            isTermination = true;
            break;
        }
        case Runtime::ReconfigurationType::SoftEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Graceful;
            isTermination = true;
            break;
        }
        case Runtime::ReconfigurationType::FailEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Failure;
            isTermination = true;
            break;
        }
        case Runtime::ReconfigurationType::ReconfigurationMarker: {
            auto marker = task.getUserData<ReconfigurationMarkerPtr>();
            auto eventOptional = getReconfigurationEventOptional(marker);

            if (!eventOptional.value()->reconfigurationMetadata->instanceOf<DrainQueryMetadata>()
                && !eventOptional.value()->reconfigurationMetadata->instanceOf<UpdateAndDrainQueryMetadata>()) {
                NES_WARNING("Other reconfigurations not yes supported");
                NES_NOT_IMPLEMENTED();
            }

            terminationType = Runtime::QueryTerminationType::Reconfiguration;
            isTermination = true;
            break;
        }
        default: {
            break;
        }
    }
    if (isTermination) {
        // TODO: #5171 uncomment event channels
        /*
        if (!workerContext.doesEventChannelExist(this->operatorId)) {
            //todo #4490: allow aborting connection here
            auto channel = workerContext.waitForAsyncConnectionEventChannel(this->operatorId);
            if (channel) {
                channel->close(terminationType);
            } else {
                NES_WARNING("Did not find a reverse event channel future; operatorId = {}", operatorId);
            }
            return;
        }
        workerContext.releaseEventOnlyChannel(this->operatorId, terminationType);*/
        // NES_ERROR("NetworkSource: reconfigure() released channel on {} Thread {}",
                  //nesPartition.toString(),
                  //Runtime::NesThread::getId());
    }
}

void NetworkSource::postReconfigurationCallback(Runtime::ReconfigurationMessage& task) {
    NES_DEBUG("NetworkSource: postReconfigurationCallback() called {}", nesPartition.toString());
    NES::DataSource::postReconfigurationCallback(task);
    Runtime::QueryTerminationType terminationType = Runtime::QueryTerminationType::Invalid;
    switch (task.getType()) {
        case Runtime::ReconfigurationType::UpdateVersion: {
            break;
        }
        case Runtime::ReconfigurationType::FailEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Failure;
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
        case Runtime::ReconfigurationType::ReconfigurationMarker: {
            auto marker = task.getUserData<ReconfigurationMarkerPtr>();
            auto eventOptional = getReconfigurationEventOptional(marker);

            if (!eventOptional.value()->reconfigurationMetadata->instanceOf<DrainQueryMetadata>()
                && !eventOptional.value()->reconfigurationMetadata->instanceOf<UpdateAndDrainQueryMetadata>()) {
                NES_WARNING("Other reconfigurations not yes supported");
                NES_NOT_IMPLEMENTED();
            }

            terminationType = Runtime::QueryTerminationType::Reconfiguration;
            break;
        }
        default: {
            break;
        }
    }
    if (Runtime::QueryTerminationType::Invalid != terminationType) {
        NES_DEBUG("NetworkSource: postReconfigurationCallback(): unregistering SubpartitionConsumer {}", nesPartition.toString());
        networkManager->unregisterSubpartitionConsumer(nesPartition);
        bool expected = true;
        if (running.compare_exchange_strong(expected, false)) {
            NES_DEBUG("NetworkSource is stopped on reconf task with id {}", nesPartition.toString());
            queryManager->notifySourceCompletion(shared_from_base<DataSource>(), terminationType);
        }
    }
}

void NetworkSource::runningRoutine(const Runtime::BufferManagerPtr&, const Runtime::QueryManagerPtr&) {
    NES_THROW_RUNTIME_ERROR("NetworkSource: runningRoutine() called, but method is invalid and should not be used.");
}

void NetworkSource::onEndOfStream(Runtime::QueryTerminationType terminationType) {
    // propagate EOS to the locally running QEPs that use the network source
    NES_DEBUG("Going to inject eos for {} terminationType={}", nesPartition, terminationType);
    if (Runtime::QueryTerminationType::Graceful == terminationType) {
        queryManager->addEndOfStream(shared_from_base<DataSource>(), Runtime::QueryTerminationType::Graceful);
    } else {
        NES_WARNING("Ignoring forceful EoS on {}", nesPartition);
    }
}

bool NetworkSource::startNewVersion() {
    NES_DEBUG("Updating version for network source {}", nesPartition);
    if (!nextSourceDescriptor) {
        return false;
    }
    networkManager->unregisterSubpartitionConsumer(nesPartition);
    auto newDescriptor = nextSourceDescriptor.value();
    version = newDescriptor.getVersion();
    sinkLocation = newDescriptor.getNodeLocation();
    nesPartition = newDescriptor.getNesPartition();
    nextSourceDescriptor = std::nullopt;
    //bind the sink to the new partition
    bind();
    auto reconfMessage = Runtime::ReconfigurationMessage(INVALID_SHARED_QUERY_ID,
                                                         INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                         INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                         Runtime::ReconfigurationType::UpdateVersion,
                                                         Reconfigurable::shared_from_this());
    queryManager->addReconfigurationMessage(INVALID_SHARED_QUERY_ID,
                                            INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                            INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                            reconfMessage,
                                            false);
    return true;
}

DecomposedQueryPlanVersion NetworkSource::getVersion() const { return version; }

void NetworkSource::onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef workerContext) {
    NES_DEBUG("NetworkSource::onEvent(event, wrkContext) called. operatorId: {}", this->operatorId);
    if (event.getEventType() == Runtime::EventType::kStartSourceEvent) {
        auto senderChannel = workerContext.getEventOnlyNetworkChannel(this->operatorId);
        if (!senderChannel) {
            auto senderChannelOptional = workerContext.getAsyncEventChannelConnectionResult(this->operatorId);
            if (!senderChannelOptional) {
                NES_DEBUG("NetworkSource::onEvent(event, wrkContext) operatorId: {}: could not send event because event channel "
                          "has not been established yet",
                          this->operatorId);
                return;
            }
            workerContext.storeEventOnlyChannel(this->operatorId, std::move(senderChannelOptional.value()));
            senderChannel = workerContext.getEventOnlyNetworkChannel(this->operatorId);
            if (!senderChannel) {
                return;
            }
        }

        senderChannel->sendEvent<Runtime::StartSourceEvent>();
    }
}

OperatorId NetworkSource::getUniqueId() const { return uniqueNetworkSourceIdentifier; }

bool NetworkSource::scheduleNewDescriptor(const NetworkSourceDescriptor& networkSourceDescriptor) {
    if (nesPartition != networkSourceDescriptor.getNesPartition()) {
        nextSourceDescriptor = networkSourceDescriptor;
        return true;
    }
    return false;
}

std::optional<ReconfigurationMarkerEventPtr> NetworkSource::getReconfigurationEventOptional(ReconfigurationMarkerPtr marker) {
    auto sourcePtr = shared_from_base<DataSource>();

    auto executablePlans = queryManager->getExecutablePlanIdsForSource(sourcePtr);

    auto executablePlanIds = std::unordered_set<DecomposedQueryId>();
    auto maxVersion = INVALID_DECOMPOSED_QUERY_PLAN_VERSION;

    for (auto& plan : executablePlans) {
        executablePlanIds.insert(plan.id);
        maxVersion = std::max(plan.version, version);
    }

    if (executablePlanIds.size() > 1) {
        NES_ERROR("Source sharing is currently not compatible with query reconfiguration");
        NES_NOT_IMPLEMENTED();
    }

    NES_ASSERT(!executablePlans.empty(), "No executable plans found for source");

    auto decomposedQueryIdWithVersion = DecomposedQueryIdWithVersion(*executablePlanIds.begin(), maxVersion);
    return marker->getReconfigurationEvent(decomposedQueryIdWithVersion);
}

bool NetworkSource::handleReconfigurationMarker(ReconfigurationMarkerPtr marker) {
    auto sourcePtr = shared_from_base<DataSource>();
    if (auto eventOptional = getReconfigurationEventOptional(marker)) {
        auto event = eventOptional.value();
        auto metadata = event->reconfigurationMetadata;
        if (metadata->instanceOf<DrainQueryMetadata>() || metadata->instanceOf<UpdateAndDrainQueryMetadata>()) {
            return queryManager->propagateReconfigurationMarker(std::move(marker), sourcePtr);
        }
        NES_ERROR("Currently not all reconfigurations are supported");
        NES_NOT_IMPLEMENTED();
    }
    return false;
}

bool NetworkSource::insertReconfigurationMarker(ReconfigurationMarkerPtr marker) {
    return handleReconfigurationMarker(std::move(marker));
}
}// namespace NES::Network
