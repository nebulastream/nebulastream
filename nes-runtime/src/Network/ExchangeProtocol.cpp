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
#include <optional>

namespace NES::Network {

// Important invariant: never leak the protocolListener pointer
// there is a hack that disables the reference counting

ExchangeProtocol::ExchangeProtocol(std::shared_ptr<PartitionManager> partitionManager,
                                   std::shared_ptr<ExchangeProtocolListener> protocolListener)
    : partitionManager(std::move(partitionManager)), protocolListener(std::move(protocolListener)) {
    NES_ASSERT(this->partitionManager, "Wrong parameter partitionManager is null");
    NES_ASSERT(this->protocolListener, "Wrong parameter ExchangeProtocolListener is null");
    NES_DEBUG("ExchangeProtocol: Initializing ExchangeProtocol()");
}

ExchangeProtocol::ExchangeProtocol(const ExchangeProtocol& other) {
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
    auto version = msg.getVersion();
    {
        auto maxSeqNumberPerNesPartitionLocked = maxSeqNumberPerNesPartition.wlock();
        auto foundVersionWaitingForDrain = false;
        for (auto& [version, info] : (*maxSeqNumberPerNesPartitionLocked)[nesPartition]) {
            if (info.expected) {
                foundVersionWaitingForDrain = true;
            }
        }
        if (foundVersionWaitingForDrain) {
            NES_WARNING("ExchangeProtocol: ClientAnnouncement received for {}, there are still versions waiting for drain", msg.getChannelId().toString());
            protocolListener->onServerError(Messages::ErrorMessage(msg.getChannelId(), ErrorType::VersionMismatchError));
            return Messages::ErrorMessage(msg.getChannelId(), ErrorType::VersionMismatchError);

        }
        if (!(*maxSeqNumberPerNesPartitionLocked)[nesPartition].contains(version)) {
            //std::pair<Util::NonBlockingMonotonicSeqQueue<uint64_t>, std::optional<uint64_t>> pair{Util::NonBlockingMonotonicSeqQueue<uint64_t>(), std::nullopt};
            SequenceInfo info{Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>(), std::nullopt, 1};
            (*maxSeqNumberPerNesPartitionLocked)[nesPartition][version] = info;
        } else {
            (*maxSeqNumberPerNesPartitionLocked)[nesPartition][version].counter++;
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

    NES_DEBUG("ExchangeProtocol: ClientAnnouncement received for {} NOT REGISTERED", msg.getChannelId().toString());
    protocolListener->onServerError(Messages::ErrorMessage(msg.getChannelId(), ErrorType::PartitionNotRegisteredError));
    return Messages::ErrorMessage(msg.getChannelId(), ErrorType::PartitionNotRegisteredError);
}

void ExchangeProtocol::onBuffer(NesPartition nesPartition, Runtime::TupleBuffer& buffer, SequenceData messageSequenceData, uint64_t sinkVersion) {
    if (partitionManager->getConsumerRegistrationStatus(nesPartition) == PartitionRegistrationStatus::Registered) {
        protocolListener->onDataBuffer(nesPartition, buffer);
        partitionManager->getDataEmitter(nesPartition)->emitWork(buffer);
        //(*maxSeqNumberPerNesPartition.wlock())[nesPartition][sinkVersion].queue.emplace(messageSequenceNumber, messageSequenceNumber);
        {
            auto lockedQueue = maxSeqNumberPerNesPartition.wlock();
            //(*lockedQueue)[nesPartition][sinkVersion].queue.emplace(messageSequenceNumber, messageSequenceNumber);
            auto& info = (*lockedQueue)[nesPartition][sinkVersion];
            info.queue.emplace(messageSequenceData, messageSequenceData.sequenceNumber);
            if (info.expected && info.expected.value() == messageSequenceData.sequenceNumber) {
                (*lockedQueue)[nesPartition].erase(sinkVersion);
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

void ExchangeProtocol::onEndOfStream(Messages::EndOfStreamMessage endOfStreamMessage) {
    const auto& eosChannelId = endOfStreamMessage.getChannelId();
    const auto& eosNesPartition = eosChannelId.getNesPartition();
    const auto& version = endOfStreamMessage.getVersion();

    if (partitionManager->getConsumerRegistrationStatus(eosNesPartition) == PartitionRegistrationStatus::Registered) {
        NES_ASSERT2_FMT(!endOfStreamMessage.isEventChannel(),
                        "Received EOS for data channel on event channel for consumer " << eosChannelId.toString());

        //decrease the counter for the version, if it bacomes 0 check if all tuples arrived and delete or mark for deletion
        {
            auto locked = maxSeqNumberPerNesPartition.wlock();
            auto& info = (*locked)[eosNesPartition][version];
            NES_ASSERT(info.counter > 0, "Sequence counter user count is 0 but still accessed");
            info.counter--;
            if (info.counter == 0) {
                const auto& eosMessageMaxSeqNumber = endOfStreamMessage.getMaxMessageSequenceNumber();

                //if all tuples are received for this version, remove the entry
                if (eosMessageMaxSeqNumber == info.queue.getCurrentValue()) {
                    (*locked)[eosNesPartition].erase(version);
                } else {
                    //set the expected count, the queue will be deleted on buffer arrivel
//                    NES_ASSERT(eosMessageMaxSeqNumber > info.queue.getCurrentValue(), "Sequence number is bigger than expected");
                    info.expected = eosMessageMaxSeqNumber;
                }
            }
        }

        const auto lastEOS = partitionManager->unregisterSubpartitionConsumer(eosNesPartition);

        //if this is the last eos,
        if (lastEOS) {
            const auto& eosMessageMaxSeqNumber = endOfStreamMessage.getMaxMessageSequenceNumber();
            while (true) {
                {
                    auto locked = maxSeqNumberPerNesPartition.wlock();
                    auto foundVersionWaitingForDrain = false;
                    for (auto& [version, info] : (*locked)[eosNesPartition]) {
                        if (info.expected) {
                            foundVersionWaitingForDrain = true;
                        }
                    }
                    if (!foundVersionWaitingForDrain) {
                        NES_DEBUG("No more versions waiting for drain");
                        if ((*locked)[eosNesPartition].empty()) {
                            NES_DEBUG("No more versions active for this partiion, erasing");
                            (*locked).erase(eosNesPartition);
                        }
                        break;
                    }
                }


                NES_DEBUG("Waiting for version to drain");
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            NES_DEBUG("Waited for all buffers for the last EOS!");
        }

        //we expect the total connection count to be the number of threads plus one registration of the source itself (happens in NetworkSource::bind())
        auto expectedTotalConnectionsInPartitionManager = endOfStreamMessage.getNumberOfSendingThreads();
        if (!lastEOS) {
            NES_DEBUG("ExchangeProtocol: EndOfStream message received on data channel from {} but there is still some active "
                      "subpartition: {}",
                      endOfStreamMessage.getChannelId().toString(),
                      *partitionManager->getSubpartitionConsumerCounter(endOfStreamMessage.getChannelId().getNesPartition()));
        } else {
            auto dataEmitter = partitionManager->getDataEmitter(endOfStreamMessage.getChannelId().getNesPartition());
            if (endOfStreamMessage.getQueryTerminationType() == Runtime::QueryTerminationType::Drain) {
                auto networkSource = std::dynamic_pointer_cast<Network::NetworkSource>(dataEmitter);
                NES_ASSERT(networkSource, "Received drain message but the data emitter is not a network source");
                networkSource->onDrainMessage(endOfStreamMessage.getNextVersion());
            } else {
                dataEmitter->onEndOfStream(endOfStreamMessage.getQueryTerminationType());
                protocolListener->onEndOfStream(endOfStreamMessage);
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