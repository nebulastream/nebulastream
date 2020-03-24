#ifndef NES_EXCHANGEPROTOCOL_HPP
#define NES_EXCHANGEPROTOCOL_HPP

#include <Network/NetworkMessage.hpp>

namespace NES {
namespace Network {
class ExchangeProtocol {
  public:
    explicit ExchangeProtocol(std::function<void()>&& onDataBuffer,
                              std::function<void()>&& onEndOfStream,
                              std::function<void(std::exception_ptr)>&& onException) :
        onDataBufferCb(std::move(onDataBuffer)),
        onEndOfStreamCb(std::move(onEndOfStream)),
        onExceptionCb(std::move(onException)) {
    }

    Messages::ServerReadyMessage onClientAnnoucement(Messages::ClientAnnounceMessage* clientAnnounceMessage);

    void onBuffer();

    void onError(std::exception_ptr ex);

    void onEndOfStream();

  private:
    std::function<void()> onDataBufferCb;
    std::function<void()> onEndOfStreamCb;
    std::function<void(std::exception_ptr)> onExceptionCb;
};
}
}

#endif //NES_EXCHANGEPROTOCOL_HPP
