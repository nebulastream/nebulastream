#include <Network/NetworkDispatcher.hpp>
#include <Network/ZmqServer.hpp>
#include <Network/OutputChannel.hpp>

namespace NES {

namespace Network {

NetworkDispatcher::NetworkDispatcher(const std::string& hostname, uint16_t port, uint16_t numServerThread) {
    server = std::make_shared<ZmqServer>(hostname, port, numServerThread);
    server->start();
}

void NetworkDispatcher::registerSubpartitionConsumer(QueryId queryId, OperatorId operatorId, PartitionId partitionId, SubpartitionId subpartitionId, std::function<void ()> consumerCallback) {

}

OutputChannel NetworkDispatcher::registerSubpartitionProducer(QueryId queryId, OperatorId operatorId, PartitionId partitionId, SubpartitionId subpartitionId) {
    return OutputChannel{server->getContext(), queryId, operatorId, partitionId, subpartitionId, "tcp://127.0.0.1:31337"};
}
}
}