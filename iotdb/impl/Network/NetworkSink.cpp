#include "Network/NetworkSink.hpp"

namespace NES {

namespace Network {

NetworkSink::NetworkSink(SchemaPtr schema, NetworkManager& networkManager, const NodeLocation& nodeLocation,
                         NesPartition nesPartition, std::chrono::seconds waitTime, uint8_t retryTimes)
    : DataSink(schema), networkManager(networkManager), nodeLocation(nodeLocation), nesPartition(nesPartition),
      waitTime(waitTime), retryTimes(retryTimes) {
}

bool NetworkSink::writeData(TupleBuffer& inputBuffer) {
    if (!outputChannel.get()) {
        auto threadId = std::hash<std::thread::id>{}(std::this_thread::get_id());
        NES_DEBUG("NetworkSink: Initializing thread specific OutputChannel " << threadId);
        auto channel = networkManager.registerSubpartitionProducer(
            nodeLocation, NesPartition{nesPartition, threadId},
            [](Messages::ErroMessage ex) {
                NES_ERROR("NetworkSink: Error in RegisterSubpartitionProducer " << ex.getErrorTypeAsString());
            },
            waitTime, retryTimes);
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

}// namespace Network

}// namespace NES
