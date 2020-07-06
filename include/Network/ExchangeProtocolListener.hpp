#ifndef NES_INCLUDE_NETWORK_EXCHANGEPROTOCOLLISTENER_HPP_
#define NES_INCLUDE_NETWORK_EXCHANGEPROTOCOLLISTENER_HPP_

#include <Network/NesPartition.hpp>
#include <Network/NetworkMessage.hpp>
#include <NodeEngine/TupleBuffer.hpp>

namespace NES {
namespace Network {
/**
 * @brief Listener for network stack events
 */
class ExchangeProtocolListener {
  public:
    /**
     * @brief This is called on every data buffer that the network stack receives
     * for a specific nes partition
     */
    virtual void onDataBuffer(NesPartition, TupleBuffer&) = 0;
    /**
     * @brief this is called once a nes partition receives an end of stream message
     */
    virtual void onEndOfStream(Messages::EndOfStreamMessage) = 0;
    /**
     * @brief this is called on the server side as soon as an error is raised
     */
    virtual void onServerError(Messages::ErrorMessage) = 0;
    /**
     * @brief This is called on the channel side as soon as an error is raised
     */
    virtual void onChannelError(Messages::ErrorMessage) = 0;
};
}
}
#endif//NES_INCLUDE_NETWORK_EXCHANGEPROTOCOLLISTENER_HPP_
