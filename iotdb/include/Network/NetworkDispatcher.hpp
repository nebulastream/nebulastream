#ifndef NES_NETWORKDISPATCHER_HPP
#define NES_NETWORKDISPATCHER_HPP

#include <cstdint>
#include <string>
#include <memory>
#include <Network/NetworkCommon.hpp>
#include <boost/core/noncopyable.hpp>
#include <functional>
#include "ExchangeProtocol.hpp"

namespace NES {

class Dispatcher;

namespace Network {

class ZmqServer;
class OutputChannel;

class NetworkDispatcher : public boost::noncopyable {
  public:

    explicit NetworkDispatcher(const std::string& hostname,
                               uint16_t port,
                               std::function<void()>&& onDataBuffer,
                               std::function<void()>&& onEndOfStream,
                               std::function<void(std::exception_ptr)>&& onError,
                               uint16_t numServerThread = DEFAULT_NUM_SERVER_THREADS);

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
}
}

#endif //NES_NETWORKDISPATCHER_HPP
