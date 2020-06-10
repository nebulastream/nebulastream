#include "Network/NetworkSink.hpp"
#include <Util/UtilityFunctions.hpp>

namespace NES {

namespace Network {

NetworkSink::NetworkSink(SchemaPtr schema, NetworkManager& networkManager, const NodeLocation& nodeLocation,
                         NesPartition nesPartition, std::chrono::seconds waitTime, uint8_t retryTimes)
    : DataSink(schema), networkManager(networkManager), nodeLocation(nodeLocation), nesPartition(nesPartition),
      waitTime(waitTime), retryTimes(retryTimes) {
}

NetworkSink::~NetworkSink() {
    NES_INFO("NetworkSink: Destructor called");
}

bool NetworkSink::writeData(TupleBuffer& inputBuffer) {
    if (!outputChannel.get()) {
        NES_DEBUG("NetworkSink: Initializing thread specific OutputChannel");
        auto channel = networkManager.registerSubpartitionProducer(
            nodeLocation, nesPartition,
            [](Messages::ErroMessage ex) {
                NES_ERROR("NetworkSink: Error in RegisterSubpartitionProducer " << ex.getErrorTypeAsString());
            },
            waitTime, retryTimes);
        outputChannel.reset(channel);
    }
    return outputChannel->sendBuffer(inputBuffer, schema->getSchemaSizeInBytes());
}

void NetworkSink::setup() {
    NES_DEBUG("NetworkSink: Empty method setup() called " << nesPartition.toString());
}

void NetworkSink::shutdown() {
    if (outputChannel.get()) {
        NES_DEBUG("NetworkSink: Shutdown called for thread specific OutputChannel.");
        outputChannel.reset();
    } else {
        NES_DEBUG("NetworkSink: Shutdown called, but no OutputChannel has been initialized.");
    }
}

SinkType NetworkSink::getType() const { return NETWORK_SINK; }

const std::string NetworkSink::toString() const {
    return "NetworkSink: " + nesPartition.toString();
}

}// namespace Network

}// namespace NES
