#include "Network/NetworkSink.hpp"
#include <Sinks/Formats/NesFormat.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES {

namespace Network {

NetworkSink::NetworkSink(
    SchemaPtr schema,
    NetworkManagerPtr networkManager,
    const NodeLocation nodeLocation,
    NesPartition nesPartition,
    BufferManagerPtr bufferManager,
    std::chrono::seconds waitTime,
    uint8_t retryTimes)
    : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager)), networkManager(std::move(networkManager)), nodeLocation(nodeLocation), nesPartition(nesPartition),
      waitTime(waitTime), retryTimes(retryTimes) {
    NES_ASSERT(this->networkManager, "Invalid network manager");
}

std::string NetworkSink::toString() {
    return "NETWORK_SINK";
}

SinkMediumTypes NetworkSink::getSinkMediumType() {
    return NETWORK_SINK;
}

NetworkSink::~NetworkSink() {
    NES_INFO("NetworkSink: Destructor called");
    shutdown();
}

bool NetworkSink::writeData(TupleBuffer& inputBuffer) {
    if (!outputChannel.get()) {
        NES_DEBUG("NetworkSink: Initializing thread specific OutputChannel");
        auto channel = networkManager->registerSubpartitionProducer(
            nodeLocation, nesPartition,
            waitTime, retryTimes);
        outputChannel.reset(channel.release());
    }

    return outputChannel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes());
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

const std::string NetworkSink::toString() const {
    return "NetworkSink: " + nesPartition.toString();
}

}// namespace Network

}// namespace NES
