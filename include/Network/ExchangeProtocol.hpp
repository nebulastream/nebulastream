#ifndef NES_EXCHANGEPROTOCOL_HPP
#define NES_EXCHANGEPROTOCOL_HPP

#include <Network/NetworkMessage.hpp>
#include <Network/PartitionManager.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <functional>

namespace NES {
namespace Network {
class ExchangeProtocolListener;
/**
 * @brief This class is used by the ZmqServer and defines the reaction for events onDataBuffer,
 * clientAnnouncement, endOfStream and exceptionHandling between all nodes of NES.
 */
class ExchangeProtocol {
  public:
    /**
     * @brief Create an exchange protocol object with a partition manager and a listener
     * @param partitionManager
     * @param listener
     */
    explicit ExchangeProtocol(
        std::shared_ptr<PartitionManager> partitionManager,
        std::shared_ptr<ExchangeProtocolListener> listener);

    ~ExchangeProtocol();

    /**
     * @brief Reaction of the zmqServer after a ClientAnnounceMessage is received.
     * @param clientAnnounceMessage
     * @return if successful, return ServerReadyMessage
     * @throw NesNetworkError if not successful
     */
    Messages::ServerReadyMessage onClientAnnouncement(Messages::ClientAnnounceMessage msg);

    /**
     * @brief Reaction of the zmqServer after a buffer is received.
     * @param id of the buffer
     * @param buffer content
     */
    void onBuffer(NesPartition id, TupleBuffer& buffer);

    /**
     * @brief Reaction of the zmqServer after an error occurs.
     * @param the error message
     */
    void onServerError(const Messages::ErrorMessage error);

    /**
     * @brief Reaction of the zmqServer after an error occurs.
     * @param the error message
     */
    void onChannelError(const Messages::ErrorMessage error);

    /**
     * @brief Reaction of the zmqServer after an EndOfStream message is received.
     * @param the endOfStreamMessage
     */
    void onEndOfStream(Messages::EndOfStreamMessage endOfStreamMessage);

    /**
     * @brief getter for the PartitionManager
     * @return the PartitionManager
     */
    std::shared_ptr<PartitionManager> getPartitionManager() const;

  private:
    std::shared_ptr<PartitionManager> partitionManager;
    std::shared_ptr<ExchangeProtocolListener> protocolListener;
};

}// namespace Network
}// namespace NES

#endif//NES_EXCHANGEPROTOCOL_HPP