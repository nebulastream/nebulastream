#include "Network/NetworkSink.hpp"

namespace NES {

namespace Network {

using namespace std;

NetworkSink::NetworkSink(SchemaPtr schema, NetworkManager& networkManager, const NodeLocation& nodeLocation,
                         NesPartition nesPartition)
    : DataSink(schema), networkManager(networkManager), nodeLocation(nodeLocation), nesPartition(nesPartition) {
}

bool NetworkSink::writeData(TupleBuffer& inputBuffer) {
    NES_DEBUG("NetworkSink: Write data called");
    if (!outputChannel.get()) {
        NES_DEBUG("NetworkSink: Initializing OutputChannel");
        nesPartition.setThreadId(std::hash<std::thread::id>{}(std::this_thread::get_id()));
        auto channel = networkManager.registerSubpartitionProducer(nodeLocation,
                                                                   nesPartition,
                                                                   [](Messages::ErroMessage ex) {},
                                                                   1,
                                                                   3);
        outputChannel.reset(channel);
    }

    return outputChannel->sendBuffer(inputBuffer);
}

void NetworkSink::setup() {
    NES_DEBUG("NetworkSink: Creating NetworkSink " << nesPartition.toString());
}

void NetworkSink::shutdown() {
    NES_DEBUG("NetworkSink: Destroying NetworkSink " << nesPartition.toString());
}

SinkType NetworkSink::getType() const { return NETWORK_SINK; }

const std::string NetworkSink::toString() const {
    return "NetworkSink: " + nesPartition.toString();
}

} // namespace Network

} // namespace NES
