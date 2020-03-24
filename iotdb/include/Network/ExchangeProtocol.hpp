#ifndef NES_EXCHANGEPROTOCOL_HPP
#define NES_EXCHANGEPROTOCOL_HPP

#include <Network/NetworkMessage.hpp>

namespace NES {
namespace Network {
class ExchangeProtocol {
public:
    Messages::ServerReadyMessage onClientAnnoucement(Messages::ClientAnnounceMessage* clientAnnounceMessage);

    void onBuffer();

    void onError();
};
}
}

#endif //NES_EXCHANGEPROTOCOL_HPP
