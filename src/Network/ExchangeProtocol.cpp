#include <Network/ExchangeProtocol.hpp>
#include <Network/ExchangeProtocolListener.hpp>
#include <utility>

namespace NES {
namespace Network {

// Important invariant: never leak the protocolListener pointer
// there is a hack that disables the reference counting

ExchangeProtocol::ExchangeProtocol(
    std::shared_ptr<PartitionManager> partitionManager,
    std::shared_ptr<ExchangeProtocolListener> protocolListener) : partitionManager(std::move(partitionManager)), protocolListener(std::move(protocolListener)) {
    NES_ASSERT(this->partitionManager, "Wrong parameter partitionManager is null");
    NES_ASSERT(this->protocolListener, "Wrong parameter ExchangeProtocolListener is null");
    NES_DEBUG("ExchangeProtocol: Initializing ExchangeProtocol()");
}

ExchangeProtocol::~ExchangeProtocol() {
    //    NES_ASSERT(protocolListener.use_count() == 1, "the protocolListener pointer was leaked");
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
        NES_DEBUG("ExchangeProtocol: ClientAnnouncement received for " << msg.getChannelId().toString() << " REGISTERED");
        // send response back to the client based on the identity
        return Messages::ServerReadyMessage(msg.getChannelId());
    } else {
        NES_ERROR("ExchangeProtocol: ClientAnnouncement received for " << msg.getChannelId().toString() << " NOT REGISTERED");
        protocolListener->onServerError(Messages::ErrorMessage(msg.getChannelId(), Messages::kPartitionNotRegisteredError));
        return Messages::ServerReadyMessage(msg.getChannelId(), Messages::kPartitionNotRegisteredError);
    }
}

void ExchangeProtocol::onBuffer(NesPartition nesPartition, TupleBuffer& buffer) {
    protocolListener->onDataBuffer(nesPartition, buffer);
}

void ExchangeProtocol::onServerError(const Messages::ErrorMessage error) {
    protocolListener->onServerError(error);
}

void ExchangeProtocol::onChannelError(const Messages::ErrorMessage error) {
    protocolListener->onChannelError(error);
}

void ExchangeProtocol::onEndOfStream(Messages::EndOfStreamMessage endOfStreamMessage) {
    NES_DEBUG("ExchangeProtocol: EndOfStream message received from " << endOfStreamMessage.getChannelId().toString());
    if (partitionManager->isRegistered(endOfStreamMessage.getChannelId().getNesPartition())) {
        partitionManager->unregisterSubpartition(endOfStreamMessage.getChannelId().getNesPartition());
    }
    protocolListener->onEndOfStream(endOfStreamMessage);
}

std::shared_ptr<PartitionManager> ExchangeProtocol::getPartitionManager() const {
    return partitionManager;
}

}// namespace Network
}// namespace NES