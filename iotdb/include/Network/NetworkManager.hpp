#ifndef NES_NETWORKDISPATCHER_HPP
#define NES_NETWORKDISPATCHER_HPP

#include <Network/ExchangeProtocol.hpp>
#include <Network/NetworkCommon.hpp>
#include <Network/OutputChannel.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <Network/PartitionManager.hpp>
#include <boost/core/noncopyable.hpp>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace NES {

namespace Network {

class ZmqServer;
class OutputChannel;

/**
 * @brief The NetworkManager manages creation and deletion of subpartition producer and consumer. This component is not ThreadSafe and should not be shared amonst threads.
 */
class NetworkManager : public boost::noncopyable {
  public:
    explicit NetworkManager(const std::string& hostname, uint16_t port,
                            std::function<void(NesPartition, TupleBuffer)>&& onDataBuffer,
                            std::function<void(NesPartition)>&& onEndOfStream,
                            std::function<void(Messages::ErroMessage)>&& onError,
                            BufferManagerPtr bufferManager, PartitionManagerPtr partitionManager,
                            uint16_t numServerThread = DEFAULT_NUM_SERVER_THREADS);

    /**
     * @brief This method is called on the receiver side to register a SubpartitionConsumer, i.e. indicate that the
     * server is ready to receive particular subpartitions.
     * @param the nesPartition
     * @return the current counter of the subpartition
     */
    uint64_t registerSubpartitionConsumer(NesPartition nesPartition);

    /**
     * @brief This method is called on the sender side to register a SubpartitionProducer. If the connection to
     * the destination server is successful, a pointer to the OutputChannel is returned, else nullptr is returned.
     * The OutputChannel is not thread safe!
     * @param nodeLocation is the destination
     * @param nesPartition indicates the partition
     * @param onError lambda which is called in case of an error
     * @param waitTime time in seconds to wait until a retry is called
     * @param retryTimes times to retry a connection
     * @return
     */
    OutputChannel* registerSubpartitionProducer(const NodeLocation& nodeLocation, NesPartition nesPartition,
                                                std::function<void(Messages::ErroMessage)>&& onError,
                                                u_int8_t waitTime, u_int8_t retryTimes);

  private:
    // TODO decide whethere unique_ptr is better here
    std::shared_ptr<ZmqServer> server;
    ExchangeProtocol exchangeProtocol;
    PartitionManagerPtr partitionManager;
};
}// namespace Network
}// namespace NES

#endif//NES_NETWORKDISPATCHER_HPP
