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

#include <Network/ExchangeProtocol.hpp>
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkSource.hpp>
#include <Network/PartitionManager.hpp>
#include <Runtime/Execution/DataEmitter.hpp>
#include <Util/Common.hpp>

namespace NES::Network {

// Important invariant: never leak the protocolListener pointer
// there is a hack that disables the reference counting

ExchangeProtocol::ExchangeProtocol(uint64_t numbOfBuffers,
                                   std::shared_ptr<PartitionManager> partitionManager,
                                   std::shared_ptr<ExchangeProtocolListener> protocolListener)
    : partitionManager(std::move(partitionManager)), protocolListener(std::move(protocolListener)) {
    this->numbOfBuffers = numbOfBuffers;
    NES_ASSERT(this->partitionManager, "Wrong parameter partitionManager is null");
    NES_ASSERT(this->protocolListener, "Wrong parameter ExchangeProtocolListener is null");
    NES_DEBUG("ExchangeProtocol: Initializing ExchangeProtocol()");
}

ExchangeProtocol::ExchangeProtocol(const ExchangeProtocol& other) {
    numbOfBuffers = other.numbOfBuffers;
    partitionManager = other.partitionManager;
    protocolListener = other.protocolListener;

    auto maxSeqNumberPerNesPartitionLocked = maxSeqNumberPerNesPartition.rlock();
    for (auto& [key, value] : (*maxSeqNumberPerNesPartitionLocked)) {
        (*maxSeqNumberPerNesPartition.wlock())[key] = value;
    }
}

std::variant<Messages::ServerReadyMessage, Messages::ErrorMessage>
ExchangeProtocol::onClientAnnouncement(Messages::ClientAnnounceMessage msg) {
    using namespace Messages;
    // check if the partition is registered via the partition manager or wait until this is not done
    // if all good, send message back
    bool isDataChannel = msg.getMode() == Messages::ChannelType::DataChannel;
    NES_INFO("ExchangeProtocol: ClientAnnouncement received for {} {}",
             msg.getChannelId().toString(),
             (isDataChannel ? "Data" : "EventOnly"));

    auto nesPartition = msg.getChannelId().getNesPartition();
    {
        auto maxSeqNumberPerNesPartitionLocked = maxSeqNumberPerNesPartition.wlock();
        if (!maxSeqNumberPerNesPartitionLocked->contains(nesPartition)) {
            (*maxSeqNumberPerNesPartitionLocked)[nesPartition] = Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>();
        }
    }

    // check if identity is registered
    if (isDataChannel) {
        // we got a connection from a data channel: it means there is/will be a partition consumer running locally
        if (auto status = partitionManager->getConsumerRegistrationStatus(nesPartition);
            status == PartitionRegistrationStatus::Registered) {

            // increment the counter
            partitionManager->pinSubpartitionConsumer(nesPartition);
            NES_DEBUG("ExchangeProtocol: ClientAnnouncement received for DataChannel {} REGISTERED",
                      msg.getChannelId().toString());
            // send response back to the client based on the identity
            return Messages::ServerReadyMessage(msg.getChannelId());
        } else if (status == PartitionRegistrationStatus::Deleted) {
            NES_WARNING("ExchangeProtocol: ClientAnnouncement received for  DataChannel {} but WAS DELETED",
                        msg.getChannelId().toString());
            protocolListener->onServerError(Messages::ErrorMessage(msg.getChannelId(), ErrorType::DeletedPartitionError));
            return Messages::ErrorMessage(msg.getChannelId(), ErrorType::DeletedPartitionError);
        }
    } else {
        // we got a connection from an event-only channel: it means there is/will be an event consumer attached to a partition producer
        if (auto status = partitionManager->getProducerRegistrationStatus(nesPartition);
            status == PartitionRegistrationStatus::Registered) {
            NES_DEBUG("ExchangeProtocol: ClientAnnouncement received for EventChannel {} REGISTERED",
                      msg.getChannelId().toString());
            partitionManager->pinSubpartitionProducer(nesPartition);
            // send response back to the client based on the identity
            return Messages::ServerReadyMessage(msg.getChannelId());
        } else if (status == PartitionRegistrationStatus::Deleted) {
            NES_WARNING("ExchangeProtocol: ClientAnnouncement received for event channel {} WAS DELETED",
                        msg.getChannelId().toString());
            protocolListener->onServerError(Messages::ErrorMessage(msg.getChannelId(), ErrorType::DeletedPartitionError));
            return Messages::ErrorMessage(msg.getChannelId(), ErrorType::DeletedPartitionError);
        }
    }

    NES_WARNING("ExchangeProtocol: ClientAnnouncement received for {} NOT REGISTERED", msg.getChannelId().toString());
    protocolListener->onServerError(Messages::ErrorMessage(msg.getChannelId(), ErrorType::PartitionNotRegisteredError));
    return Messages::ErrorMessage(msg.getChannelId(), ErrorType::PartitionNotRegisteredError);
}

void ExchangeProtocol::onBuffer(NesPartition nesPartition, Runtime::TupleBuffer& buffer, SequenceData messageSequenceData) {
    if (partitionManager->getConsumerRegistrationStatus(nesPartition) == PartitionRegistrationStatus::Registered) {
        (*maxSeqNumberPerNesPartition.wlock())[nesPartition].emplace(messageSequenceData, messageSequenceData.sequenceNumber);
        NES_DEBUG("buffer {} received for {}", messageSequenceData.sequenceNumber, nesPartition.toString())
        protocolListener->onDataBuffer(nesPartition, buffer);
        partitionManager->getDataEmitter(nesPartition)->emitWork(buffer, false);
        if (numbOfBuffers > 0) {
            if (messageSequenceData.sequenceNumber == 1) {
                NES_ERROR("started receiving tuples {}", messageSequenceData.sequenceNumber);
            } else if (messageSequenceData.sequenceNumber == numbOfBuffers - 1) {
                NES_ERROR("finished receiving tuples {}", messageSequenceData.sequenceNumber);
            }
        }
    } else {
        NES_ERROR("DataBuffer for {} is not registered and was discarded!", nesPartition.toString());
    }
}

void ExchangeProtocol::onServerError(const Messages::ErrorMessage error) { protocolListener->onServerError(error); }

void ExchangeProtocol::onChannelError(const Messages::ErrorMessage error) { protocolListener->onChannelError(error); }

void ExchangeProtocol::onEvent(NesPartition nesPartition, Runtime::BaseEvent& event) {
    if (partitionManager->getConsumerRegistrationStatus(nesPartition) == PartitionRegistrationStatus::Registered) {
        protocolListener->onEvent(nesPartition, event);
        partitionManager->getDataEmitter(nesPartition)->onEvent(event);
    } else if (partitionManager->getProducerRegistrationStatus(nesPartition) == PartitionRegistrationStatus::Registered) {
        protocolListener->onEvent(nesPartition, event);
        if (auto listener = partitionManager->getEventListener(nesPartition); listener != nullptr) {
            listener->onEvent(event);
        }
    } else {
        NES_ERROR("DataBuffer for {} is not registered and was discarded!", nesPartition.toString());
    }
}

void ExchangeProtocol::onEndOfStream(Messages::EndOfStreamMessage endOfStreamMessage,
                                     std::optional<ReconfigurationMarkerPtr> marker) {
    const auto& eosChannelId = endOfStreamMessage.getChannelId();
    const auto& eosNesPartition = eosChannelId.getNesPartition();

    if (partitionManager->getConsumerRegistrationStatus(eosNesPartition) == PartitionRegistrationStatus::Registered) {
        NES_ASSERT2_FMT(!endOfStreamMessage.isEventChannel(),
                        "Received EOS for data channel on event channel for consumer " << eosChannelId.toString());

        const auto lastEOS = partitionManager->unregisterSubpartitionConsumer(eosNesPartition, endOfStreamMessage.getMaxMessageSequenceNumber());
        NES_DEBUG("Received lastEOS {}, max message seq number {} and max stored seq number {} for parition {}", lastEOS, endOfStreamMessage.getMaxMessageSequenceNumber(), partitionManager->getMaxRegisteredSequenceNumber(eosNesPartition), eosNesPartition.getOperatorId());
        if (lastEOS) {
//
            const auto& eosMessageMaxSeqNumber = partitionManager->getMaxRegisteredSequenceNumber(eosNesPartition);
            while ((*maxSeqNumberPerNesPartition.rlock()).at(eosNesPartition).getCurrentValue() < eosMessageMaxSeqNumber) {
                NES_ERROR("Current message sequence number {} is less than expected max {} for partition {}",
                          (*maxSeqNumberPerNesPartition.rlock()).at(eosNesPartition).getCurrentValue(),
                          eosMessageMaxSeqNumber,
                          eosNesPartition);
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            NES_DEBUG("Waited for all buffers for the last EOS!");

            // Cleaning up and resetting for this partition, so that we can reuse the partition later on
            (*maxSeqNumberPerNesPartition.wlock())[eosNesPartition] = Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>();
        }

        //we expect the total connection count to be the number of threads plus one registration of the source itself (happens in NetworkSource::bind())
        auto expectedTotalConnectionsInPartitionManager = endOfStreamMessage.getNumberOfSendingThreads();
        if (!lastEOS) {
            NES_WARNING("ExchangeProtocol: EndOfStream message received on data channel from {} but there is still some active "
                        "subpartition: {}",
                        endOfStreamMessage.getChannelId().toString(),
                        *partitionManager->getSubpartitionConsumerCounter(endOfStreamMessage.getChannelId().getNesPartition()));
        } else {
            auto dataEmitter = partitionManager->getDataEmitter(endOfStreamMessage.getChannelId().getNesPartition());
            auto networkSource = std::dynamic_pointer_cast<Network::NetworkSource>(dataEmitter);
            if (!(networkSource && networkSource->startNewVersion())) {
                if (endOfStreamMessage.getQueryTerminationType() == Runtime::QueryTerminationType::Reconfiguration) {
                    if (marker) {
                        dataEmitter->insertReconfigurationMarker(std::move(marker.value()));
                    }
                } else {
                    dataEmitter->onEndOfStream(endOfStreamMessage.getQueryTerminationType());
                    protocolListener->onEndOfStream(endOfStreamMessage);
                }
            }
        }
    } else if (partitionManager->getProducerRegistrationStatus(eosNesPartition) == PartitionRegistrationStatus::Registered) {
        NES_ASSERT2_FMT(endOfStreamMessage.isEventChannel(),
                        "Received EOS for event channel on data channel for producer " << eosChannelId.toString());
        if (partitionManager->unregisterSubpartitionProducer(eosNesPartition)) {
            NES_DEBUG("ExchangeProtocol: EndOfStream message received from event channel {} but with no active subpartition",
                      eosChannelId.toString());
        } else {
            NES_DEBUG("ExchangeProtocol: EndOfStream message received from event channel {} but there is still some active "
                      "subpartition: {}",
                      eosChannelId.toString(),
                      *partitionManager->getSubpartitionProducerCounter(eosNesPartition));
        }
    } else {
        NES_ERROR("ExchangeProtocol: EndOfStream message received from {} however the partition is not registered on this worker",
                  eosChannelId.toString());
        protocolListener->onServerError(Messages::ErrorMessage(eosChannelId, Messages::ErrorType::UnknownPartitionError));
    }
}

std::shared_ptr<PartitionManager> ExchangeProtocol::getPartitionManager() const { return partitionManager; }

}// namespace NES::Network
