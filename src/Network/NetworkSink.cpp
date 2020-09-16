#include "Network/NetworkSink.hpp"
#include <Sinks/Formats/NesFormat.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES {

namespace Network {

NetworkSink::NetworkSink(
    SchemaPtr schema,
    QuerySubPlanId parentPlanId,
    NetworkManagerPtr networkManager,
    const NodeLocation nodeLocation,
    NesPartition nesPartition,
    BufferManagerPtr bufferManager,
    QueryManagerPtr queryManager,
    std::chrono::seconds waitTime,
    uint8_t retryTimes)
    : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), parentPlanId),
      networkManager(std::move(networkManager)), nodeLocation(nodeLocation),
      nesPartition(nesPartition), queryManager(queryManager),
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

bool NetworkSink::writeData(TupleBuffer& inputBuffer, WorkerContext& workerContext) {
    auto* channel = workerContext.getChannel(nesPartition.getOperatorId());
    NES_VERIFY(channel, "invalid channel on " << nesPartition);
    return channel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes());
}

void NetworkSink::setup() {
    NES_DEBUG("NetworkSink: Empty method setup() called " << nesPartition.toString());
    queryManager->addReconfigurationTask(parentPlanId, ReconfigurationTask(Initialize, this));
}

void NetworkSink::shutdown() {
}

const std::string NetworkSink::toString() const {
    return "NetworkSink: " + nesPartition.toString();
}

void NetworkSink::reconfigure(WorkerContext& workerContext) {
    Reconfigurable::reconfigure(workerContext);
    auto channel = networkManager->registerSubpartitionProducer(
        nodeLocation, nesPartition,
        waitTime, retryTimes);
    workerContext.storeChannel(nesPartition.getOperatorId(), std::move(channel));
}

}// namespace Network

}// namespace NES
