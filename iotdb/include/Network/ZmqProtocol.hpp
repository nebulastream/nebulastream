#ifndef NES_ZMQPROTOCOL_HPP
#define NES_ZMQPROTOCOL_HPP

class ZmqServerProtocol {
public:
    void onClientAnnoucement();
    void onBuffer();
    void onError();
};

#endif //NES_ZMQPROTOCOL_HPP
