#include <Network/ExchangeProtocol.hpp>

namespace NES {
namespace Network {

Messages::ServerReadyMessage ExchangeProtocol::onClientAnnoucement(Messages::ClientAnnounceMessage* clientAnnounceMessage) {
    // check if the partition is registered via the partition manager or wait until this is not done

    // if all good, send message back

    return Messages::ServerReadyMessage{clientAnnounceMessage->getQueryId(), clientAnnounceMessage->getOperatorId(),
                                        clientAnnounceMessage->getPartitionId(),
                                        clientAnnounceMessage->getSubpartitionId()};
}

void ExchangeProtocol::onBuffer() {
    onDataBufferCb();
}

void ExchangeProtocol::onError(std::exception_ptr ex) {
    onExceptionCb(ex);
}

void ExchangeProtocol::onEndOfStream() {
    onEndOfStreamCb();
}

}
}