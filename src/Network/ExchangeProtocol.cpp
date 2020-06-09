#include <Network/ExchangeProtocol.hpp>

namespace NES {
namespace Network {

ExchangeProtocol::ExchangeProtocol(BufferManagerPtr bufferManager,
                                   PartitionManagerPtr partitionManager,
                                   QueryManagerPtr queryManager,
                                   std::function<void(NesPartition, TupleBuffer&)>&& onDataBuffer,
                                   std::function<void(Messages::EndOfStreamMessage)>&& onEndOfStream,
                                   std::function<void(Messages::ErroMessage)>&& onException) : bufferManager(bufferManager), partitionManager(partitionManager), queryManager(queryManager),
                                                                                               onDataBufferCallback(std::move(onDataBuffer)),
                                                                                               onEndOfStreamCallback(std::move(onEndOfStream)),
                                                                                               onExceptionCallback(std::move(onException)) {
    NES_DEBUG("ExchangeProtocol: Initializing ExchangeProtocol()");
    if (!onDataBufferCallback) {
        NES_THROW_RUNTIME_ERROR("ExchangeProtocol: OnDataBuffer not initialized.");
    }
    if (!onEndOfStreamCallback) {
        NES_THROW_RUNTIME_ERROR("ExchangeProtocol: onEndOfStreamCallback not initialized.");
    }
    if (!onExceptionCallback) {
        NES_THROW_RUNTIME_ERROR("ExchangeProtocol: onExceptionCallback not initialized.");
    }
}

Messages::ServerReadyMessage ExchangeProtocol::onClientAnnouncement(Messages::ClientAnnounceMessage msg) {
    // check if the partition is registered via the partition manager or wait until this is not done
    // if all good, send message back
    NES_INFO("ExchangeProtocol: ClientAnnouncement received for " << msg.getChannelId().toString());
    auto nesPartition = msg.getChannelId().getNesPartition();

    // check if identity is registered
    if (partitionManager->isRegistered(nesPartition)) {
        // increment the counter
        partitionManager->registerSubpartition(nesPartition);
        // send response back to the client based on the identity
        return Messages::ServerReadyMessage{msg.getChannelId()};
    } else {
        auto errorMsg = Messages::ErroMessage{msg.getChannelId(), Messages::PartitionNotRegisteredError};
        throw Messages::NesNetworkError(errorMsg);
    }
}

void ExchangeProtocol::onBuffer(NesPartition nesPartition, TupleBuffer& buffer) {
    // check if identity is registered
    if (partitionManager->isRegistered(nesPartition)) {
        // create a string for logging of the identity which corresponds to the
        // queryId::operatorId::partitionId::subpartitionId
        //TODO: dont use strings for lookups
        queryManager->addWork(std::to_string(nesPartition.getQueryId()), buffer);
    } else {
        // partition is not registered, discard the buffer
        buffer.release();
        NES_ERROR("ExchangeProtocol: "
                  << "DataBuffer for " + nesPartition.toString()
                      + " is not registered and was discarded!");
    }
    onDataBufferCallback(nesPartition, buffer);
}

Messages::ErroMessage ExchangeProtocol::onError(const Messages::ErroMessage error) {
    onExceptionCallback(error);
    return error;
}

void ExchangeProtocol::onEndOfStream(Messages::EndOfStreamMessage endOfStreamMessage) {
    NES_INFO("ExchangeProtocol: EndOfStream message received from " << endOfStreamMessage.getChannelId().toString());
    if (partitionManager->isRegistered(endOfStreamMessage.getChannelId().getNesPartition())) {
        partitionManager->unregisterSubpartition(endOfStreamMessage.getChannelId().getNesPartition());
    }
    onEndOfStreamCallback(endOfStreamMessage);
}

BufferManagerPtr ExchangeProtocol::getBufferManager() const {
    return bufferManager;
}

PartitionManagerPtr ExchangeProtocol::getPartitionManager() const {
    return partitionManager;
}

QueryManagerPtr ExchangeProtocol::getQueryManager() const {
    return queryManager;
}

}// namespace Network
}// namespace NES