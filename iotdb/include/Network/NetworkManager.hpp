#ifndef NES_NETWORKDISPATCHER_HPP
#define NES_NETWORKDISPATCHER_HPP

#include "ExchangeProtocol.hpp"
#include <Network/NetworkCommon.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <boost/core/noncopyable.hpp>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace NES {

namespace Network {

class ZmqServer;
class OutputChannel;

class NetworkManager : public boost::noncopyable {
  public:
    explicit NetworkManager(const std::string& hostname, uint16_t port,
                            std::function<void(uint32_t*, TupleBuffer)>&& onDataBuffer,
                            std::function<void()>&& onEndOfStream, std::function<void(std::exception_ptr)>&& onError,
                            BufferManagerPtr bufferManager, uint16_t numServerThread = DEFAULT_NUM_SERVER_THREADS);

    void registerSubpartitionConsumer(QueryId queryId,
                                      OperatorId operatorId,
                                      PartitionId partitionId,
                                      SubpartitionId subpartitionId);

    OutputChannel registerSubpartitionProducer(const NodeLocation& nodeLocation, QueryId queryId,
                                               OperatorId operatorId,
                                               PartitionId partitionId,
                                               SubpartitionId subpartitionId);

  private:
    // TODO decide whethere unique_ptr is better here
    std::shared_ptr<ZmqServer> server;
    ExchangeProtocol exchangeProtocol;
};
} // namespace Network
} // namespace NES

#endif //NES_NETWORKDISPATCHER_HPP
