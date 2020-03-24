#ifndef NES_NETWORKMESSAGE_HPP
#define NES_NETWORKMESSAGE_HPP

#include <cstdint>

namespace NES {
namespace Network {
namespace Messages {

    using nes_magic_number_t = uint32_t;
    static constexpr nes_magic_number_t NES_NETWORK_MAGIC_NUMBER = 0xBADC0FFEE;

    enum MessageType {
        kClientAnnouncement,
        kServerReady,
        kDataBuffer,
        kErrorMessage
    };

    class MessageHeader {
    public:
        explicit MessageHeader(nes_magic_number_t magicNumber, MessageType msgType, uint32_t msgLength)
            : magicNumber(magicNumber), msgType(msgType), msgLength(msgLength) {
        }

    private:
        nes_magic_number_t magicNumber;
        MessageType msgType;
        uint32_t msgLength;
    };

    class ClientAnnounceMessage {
    public:
    private:
    };

    class ServerReadyMessage {
    public:
    private:
    };

    class DataBufferMessage {
    public:
    private:
    };

    class ErroMessage {
    public:
    private:
    };


}
}
}

#endif //NES_NETWORKMESSAGE_HPP
