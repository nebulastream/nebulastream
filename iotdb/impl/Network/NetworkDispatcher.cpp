#include <Network/NetworkDispatcher.hpp>
#include <Network/ZmqServer.hpp>
#include <Network/OutputChannel.hpp>

namespace NES {

namespace Network {

NetworkDispatcher::NetworkDispatcher(const std::string& hostname,
                                     uint16_t port,
                                     std::function<void()>&& onDataBuffer,
                                     std::function<void()>&& onEndOfStream,
                                     std::function<void(std::exception_ptr)>&& onError,
                                     uint16_t numServerThread) :
    exchangeProtocol(std::move(onDataBuffer), std::move(onEndOfStream), std::move(onError)),
    server(std::make_shared<ZmqServer>(hostname, port, numServerThread, exchangeProtocol)) {
    server->start();
}

void NetworkDispatcher::registerSubpartitionConsumer(QueryId queryId,
                                                     OperatorId operatorId,
                                                     PartitionId partitionId,
                                                     SubpartitionId subpartitionId) {

}

OutputChannel NetworkDispatcher::registerSubpartitionProducer(const NodeLocation& nodeLocation, QueryId queryId,
                                                              OperatorId operatorId,
                                                              PartitionId partitionId,
                                                              SubpartitionId subpartitionId) {
    //TODO: we need to look up the right socket address
    return OutputChannel{server->getContext(), queryId, operatorId, partitionId, subpartitionId,
                         nodeLocation.createZmqURI()};
}

}
}