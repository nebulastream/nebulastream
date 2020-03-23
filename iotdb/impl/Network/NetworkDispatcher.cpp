#include <Network/NetworkDispatcher.hpp>
#include <Network/ZmqServer.hpp>

namespace NES {

namespace Network {

NetworkDispatcher::NetworkDispatcher(const std::string& hostname, uint16_t port, uint16_t numServerThread) {
    server = std::make_shared<ZmqServer>(hostname, port, numServerThread);
    server->start();
}

void NetworkDispatcher::registerPartition(QueryId queryId, OperatorId operatorId, PartitionId partitionId, SubpartitionId subpartitionId) {
    
}

InputChannel NetworkDispatcher::getInputChannel(QueryId queryId, OperatorId operatorId, PartitionId partitionId, SubpartitionId subpartitionId) {

}

OutputChannel NetworkDispatcher::getOutputChannel(QueryId queryId, OperatorId operatorId, PartitionId partitionId, SubpartitionId subpartitionId) {}

}

}
