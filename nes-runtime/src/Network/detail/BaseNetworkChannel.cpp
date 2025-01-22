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

#include <Network/NetworkMessage.hpp>
#include <Network/ZmqUtils.hpp>
#include <Network/detail/BaseNetworkChannel.hpp>
#include <Reconfiguration/Metadata/DrainQueryMetadata.hpp>
#include <Reconfiguration/Metadata/UpdateAndDrainQueryMetadata.hpp>
#include <Reconfiguration/Metadata/UpdateQueryMetadata.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Network::detail {
BaseNetworkChannel::BaseNetworkChannel(zmq::socket_t&& zmqSocket,
                                       const ChannelId channelId,
                                       std::string&& address,
                                       Runtime::BufferManagerPtr&& bufferManager)
    : socketAddr(std::move(address)), zmqSocket(std::move(zmqSocket)), channelId(channelId),
      bufferManager(std::move(bufferManager)) {}

void BaseNetworkChannel::onError(Messages::ErrorMessage& errorMsg) { NES_ERROR("{}", errorMsg.getErrorTypeAsString()); }

void BaseNetworkChannel::close(bool isEventOnly,
                               Runtime::QueryTerminationType terminationType,
                               uint16_t numSendingThreads,
                               uint64_t currentMessageSequenceNumber,
                               bool shouldPropagateMarker,
                               const std::optional<ReconfigurationMarkerPtr>& reconfigurationMarker) {

    auto events = 0;
    if (terminationType == Runtime::QueryTerminationType::Reconfiguration && !isEventOnly) {
        NES_ASSERT((reconfigurationMarker.has_value()),
                   "Reconfiguration marker must be provided for reconfiguration of data channels");
        if (shouldPropagateMarker) {
            events = reconfigurationMarker.value()->getAllReconfigurationMarkerEvents().size();
        }
    } else {
        NES_ASSERT(!reconfigurationMarker.has_value(), "Reconfiguration marker must not be provided for non-reconfiguration");
    }

    if (isClosed) {
        return;
    }
    if (isEventOnly) {
        sendMessage<Messages::EndOfStreamMessage>(zmqSocket,
                                                  channelId,
                                                  Messages::ChannelType::EventOnlyChannel,
                                                  terminationType,
                                                  numSendingThreads,
                                                  currentMessageSequenceNumber);
    } else {
        //check if this is a reconfiguration, if yes, also propagate the reconfiguration events
        if (!reconfigurationMarker || !shouldPropagateMarker) {
            //not a reconfiguration, no events to propagate

            //todo #4313: pass number of threads on client announcement instead of on closing
            NES_ASSERT(events == 0, "There should be 0 events in this case, as reconfiguration marker is null");
            // NES_ERROR("sent eof for {}", channelId);
            sendMessage<Messages::EndOfStreamMessage>(zmqSocket,
                                                      channelId,
                                                      Messages::ChannelType::DataChannel,
                                                      terminationType,
                                                      numSendingThreads,
                                                      currentMessageSequenceNumber,
                                                      events);
        } else {
            //reconfiguration, create message for each reconfiguration event
            sendMessage<Messages::EndOfStreamMessage, kZmqSendMore>(zmqSocket,
                                                                    channelId,
                                                                    Messages::ChannelType::DataChannel,
                                                                    terminationType,
                                                                    numSendingThreads,
                                                                    currentMessageSequenceNumber,
                                                                    events);

            auto count = 0;
            for (auto& [key, msg] : reconfigurationMarker.value()->getAllReconfigurationMarkerEvents()) {

                QueryState queryState = msg->queryState;
                ReconfigurationMetadataType metadataType;
                uint16_t numberOfSources;
                WorkerId workerId = INVALID_WORKER_NODE_ID;
                SharedQueryId sharedQueryId = INVALID_SHARED_QUERY_ID;
                DecomposedQueryId decomposedQueryId = INVALID_DECOMPOSED_QUERY_PLAN_ID;
                DecomposedQueryPlanVersion decomposedQueryPlanVersion;

                auto metadata = msg->reconfigurationMetadata;
                if (metadata->instanceOf<DrainQueryMetadata>()) {
                    auto drain = metadata->as<DrainQueryMetadata>();
                    metadataType = ReconfigurationMetadataType::DrainQuery;
                    numberOfSources = drain->numberOfSources;
                }
                if (metadata->instanceOf<UpdateQueryMetadata>()) {
                    auto update = metadata->as<UpdateQueryMetadata>();
                    metadataType = ReconfigurationMetadataType::UpdateQuery;
                    workerId = update->workerId;
                    sharedQueryId = update->sharedQueryId;
                    decomposedQueryId = update->decomposedQueryId;
                    decomposedQueryPlanVersion = update->decomposedQueryPlanVersion;
                }
                if (metadata->instanceOf<UpdateAndDrainQueryMetadata>()) {
                    auto updateAndDrain = metadata->as<UpdateAndDrainQueryMetadata>();
                    metadataType = ReconfigurationMetadataType::UpdateAndDrainQuery;
                    workerId = updateAndDrain->workerId;
                    sharedQueryId = updateAndDrain->sharedQueryId;
                    decomposedQueryId = updateAndDrain->decomposedQueryId;
                    decomposedQueryPlanVersion = updateAndDrain->decomposedQueryPlanVersion;
                    numberOfSources = updateAndDrain->numberOfSources;
                }

                //check if we reached the last element
                if (count < events - 1) {
                    //if this is not the last message, send with send more flag
                    sendMessageNoHeader<Messages::ReconfigurationEventMessage, kZmqSendMore>(zmqSocket,
                                                                                             key,
                                                                                             queryState,
                                                                                             metadataType,
                                                                                             numberOfSources,
                                                                                             workerId,
                                                                                             sharedQueryId,
                                                                                             decomposedQueryId,
                                                                                             decomposedQueryPlanVersion);
                } else {
                    //send the last element without send more flag
                    sendMessageNoHeader<Messages::ReconfigurationEventMessage>(zmqSocket,
                                                                               key,
                                                                               queryState,
                                                                               metadataType,
                                                                               numberOfSources,
                                                                               workerId,
                                                                               sharedQueryId,
                                                                               decomposedQueryId,
                                                                               decomposedQueryPlanVersion);
                }
                ++count;
            }
        }
    }
    // sleep(1);
    zmqSocket.close();
    NES_DEBUG("Socket(\"{}\") closed for {} {}", socketAddr, channelId, (isEventOnly ? " Event" : " Data"));
    isClosed = true;
}
}// namespace NES::Network::detail
