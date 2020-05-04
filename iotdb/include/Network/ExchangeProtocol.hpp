#ifndef NES_EXCHANGEPROTOCOL_HPP
#define NES_EXCHANGEPROTOCOL_HPP

#include <functional>
#include <Network/NetworkMessage.hpp>
#include <NodeEngine/TupleBuffer.hpp>

namespace NES {
namespace Network {
class ExchangeProtocol {
  public:
    explicit ExchangeProtocol(std::function<void(uint64_t*, TupleBuffer)>&& onDataBuffer,
                              std::function<void()>&& onEndOfStream,
                              std::function<void(std::exception_ptr)>&& onException) :
        onDataBufferCb(std::move(onDataBuffer)),
        onEndOfStreamCb(std::move(onEndOfStream)),
        onExceptionCb(std::move(onException)) {
    }

    /**
     *
     * @param clientAnnounceMessage
     * @return
     */
    Messages::ServerReadyMessage onClientAnnouncement(Messages::ClientAnnounceMessage* clientAnnounceMessage) {
        // check if the partition is registered via the partition manager or wait until this is not done

        // if all good, send message back

        return Messages::ServerReadyMessage{clientAnnounceMessage->getQueryId(), clientAnnounceMessage->getOperatorId(),
                                            clientAnnounceMessage->getPartitionId(),
                                            clientAnnounceMessage->getSubpartitionId()};
    }

    /**
     *
     */
    void onBuffer(uint64_t* id, TupleBuffer buffer) {
        onDataBufferCb(id, buffer);
    }

    /**
     *
     * @param ex
     */
    void onError(std::exception_ptr ex) {
        onExceptionCb(ex);
    }

    /**
     *
     */
    void onEndOfStream() {
        onEndOfStreamCb();
    }

  private:
    std::function<void(uint64_t*, TupleBuffer)> onDataBufferCb;
    std::function<void()> onEndOfStreamCb;
    std::function<void(std::exception_ptr)> onExceptionCb;
};

}
}

#endif //NES_EXCHANGEPROTOCOL_HPP
