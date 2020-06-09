#ifndef NES_EXCHANGEPROTOCOL_HPP
#define NES_EXCHANGEPROTOCOL_HPP

#include <Network/NetworkMessage.hpp>
#include <Network/PartitionManager.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <functional>

namespace NES {
namespace Network {

/**
 * @brief This class is used by the ZmqServer and defines the reaction for events onDataBuffer,
 * clientAnnouncement, endOfStream and exceptionHandling between all nodes of NES.
 */
class ExchangeProtocol {
  public:
    explicit ExchangeProtocol(
        BufferManagerPtr bufferManager, PartitionManagerPtr partitionManager,
        QueryManagerPtr queryManager,
        std::function<void(NesPartition, TupleBuffer&)>&& onDataBuffer = [](NesPartition id,
                                                                            TupleBuffer& buf) {
        },
        std::function<void(Messages::EndOfStreamMessage)>&& onEndOfStream = [](Messages::EndOfStreamMessage p) {
        },
        std::function<void(Messages::ErroMessage)>&& onException = [](Messages::ErroMessage ex) {
        });

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
     * @return the handled error message
     */
    Messages::ErroMessage onError(const Messages::ErroMessage error);

    /**
     * @brief Reaction of the zmqServer after an EndOfStream message is received.
     * @param the endOfStreamMessage
     */
    void onEndOfStream(Messages::EndOfStreamMessage endOfStreamMessage);

    /**
     * @brief getter for the BufferManager
     * @return the buffer manager
     */
    BufferManagerPtr getBufferManager() const;

    /**
     * @brief getter for the PartitionManager
     * @return the PartitionManager
     */
    PartitionManagerPtr getPartitionManager() const;

    /**
     * @brief getter for the QueryManager
     * @return the QueryManager
     */
    QueryManagerPtr getQueryManager() const;

  private:
    BufferManagerPtr bufferManager;
    PartitionManagerPtr partitionManager;
    QueryManagerPtr queryManager;

    std::function<void(NesPartition, TupleBuffer&)> onDataBufferCallback;
    std::function<void(Messages::EndOfStreamMessage)> onEndOfStreamCallback;
    std::function<void(Messages::ErroMessage)> onExceptionCallback;
};
typedef std::shared_ptr<ExchangeProtocol> ExchangeProtocolPtr;

}// namespace Network
}// namespace NES

#endif//NES_EXCHANGEPROTOCOL_HPP
