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
                            std::function<void(uint64_t*, TupleBuffer)>&& onDataBuffer,
                            std::function<void(NesPartition)>&& onEndOfStream,
                            std::function<void(Messages::ErroMessage)>&& onError,
                            BufferManagerPtr bufferManager, PartitionManagerPtr partitionManager,
                            uint16_t numServerThread = DEFAULT_NUM_SERVER_THREADS);

    void registerSubpartitionConsumer(QueryId queryId,
                                      OperatorId operatorId,
                                      PartitionId partitionId,
                                      SubpartitionId subpartitionId);

    OutputChannel* registerSubpartitionProducer(const NodeLocation& nodeLocation, NesPartition nesPartition,
                                                std::function<void(Messages::ErroMessage)>&& onError,
                                                u_int64_t waitTime, u_int64_t retryTimes);

  private:
    // TODO decide whethere unique_ptr is better here
    std::shared_ptr<ZmqServer> server;
    ExchangeProtocol exchangeProtocol;
    PartitionManagerPtr partitionManager;
};
}// namespace Network
}// namespace NES

#endif//NES_NETWORKDISPATCHER_HPP
