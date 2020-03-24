#ifndef NES_NETWORKDISPATCHER_HPP
#define NES_NETWORKDISPATCHER_HPP

#include <cstdint>
#include <string>
#include <memory>
#include <Network/NetworkCommon.hpp>
#include <boost/core/noncopyable.hpp>
#include <functional>

namespace NES {

class Dispatcher;

namespace Network {

class ZmqServer;
class OutputChannel;

class NetworkDispatcher : public boost::noncopyable {
public:

    explicit NetworkDispatcher(const std::string& hostname, uint16_t port, uint16_t numServerThread = DEFAULT_NUM_SERVER_THREADS);

    void registerSubpartitionConsumer(QueryId queryId, OperatorId operatorId, PartitionId partitionId, SubpartitionId subpartitionId, std::function<void(void)> consumerCallback);

    OutputChannel registerSubpartitionProducer(QueryId queryId, OperatorId operatorId, PartitionId partitionId, SubpartitionId subpartitionId);

private:
    // TODO decide whethere unique_ptr is better here
    std::shared_ptr<ZmqServer> server;

};
}
}

#endif //NES_NETWORKDISPATCHER_HPP
