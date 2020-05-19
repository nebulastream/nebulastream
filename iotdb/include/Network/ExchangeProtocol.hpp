#ifndef NES_EXCHANGEPROTOCOL_HPP
#define NES_EXCHANGEPROTOCOL_HPP

#include <Network/NetworkMessage.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <functional>

namespace NES {
namespace Network {
class ExchangeProtocol {
  public:
    explicit ExchangeProtocol(std::function<void(NesPartition, TupleBuffer&)>&& onDataBuffer,
                              std::function<void(NesPartition)>&& onEndOfStream,
                              std::function<void(Messages::ErroMessage)>&& onException) : onDataBufferCb(std::move(onDataBuffer)),
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
    void onBuffer(NesPartition id, TupleBuffer buffer) {
        onDataBufferCb(id, buffer);
    }

    /**
     *
     * @param ex
     */
    Messages::ErroMessage onError(const Messages::ErroMessage error) {
        onExceptionCb(error);
        return error;
    }

    /**
     *
     */
    void onEndOfStream(NesPartition subpartition) {
        onEndOfStreamCb(subpartition);
    }

  private:
    std::function<void(NesPartition, TupleBuffer&)> onDataBufferCb;
    std::function<void(NesPartition subpartition)> onEndOfStreamCb;
    std::function<void(Messages::ErroMessage)> onExceptionCb;
};

}// namespace Network
}// namespace NES

#endif//NES_EXCHANGEPROTOCOL_HPP
