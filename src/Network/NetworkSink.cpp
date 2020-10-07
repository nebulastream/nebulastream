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
    NES_DEBUG("NetworkSink: Created NetworkSink for partition " << nesPartition << " location " << nodeLocation.createZmqURI());
}

std::string NetworkSink::toString() {
    return "NETWORK_SINK";
}

SinkMediumTypes NetworkSink::getSinkMediumType() {
    return NETWORK_SINK;
}

NetworkSink::~NetworkSink() {
    NES_INFO("NetworkSink: Destructor called " << nesPartition);
}

bool NetworkSink::writeData(TupleBuffer& inputBuffer, WorkerContext& workerContext) {
    auto* channel = workerContext.getChannel(nesPartition.getOperatorId());
    NES_VERIFY(channel, "invalid channel on " << nesPartition);
    return channel->sendBuffer(inputBuffer, sinkFormat->getSchemaPtr()->getSchemaSizeInBytes());
}

void NetworkSink::setup() {
    NES_DEBUG("NetworkSink: method setup() called " << nesPartition.toString() << " qep " << parentPlanId);
//    NES_ASSERT(queryManager->getQepStatus(parentPlanId) == QueryExecutionPlan::Created, "Setup : parent plan not running on net sink " << nesPartition);
    queryManager->addReconfigurationTask(parentPlanId, ReconfigurationTask(parentPlanId, Initialize, this));
}

void NetworkSink::shutdown() {
    NES_DEBUG("NetworkSink: shutdown() called " << nesPartition.toString() << " qep " << parentPlanId);
    queryManager->addReconfigurationTask(parentPlanId, ReconfigurationTask(parentPlanId, Destroy, this), true);
}

const std::string NetworkSink::toString() const {
    return "NetworkSink: " + nesPartition.toString();
}

void NetworkSink::reconfigure(ReconfigurationTask& task, WorkerContext& workerContext) {
    NES_DEBUG("NetworkSink: reconfigure() called " << nesPartition.toString() << " parent plan " << parentPlanId);
    Reconfigurable::reconfigure(task, workerContext);
    switch (task.getType()) {
        case Initialize: {
//            NES_ASSERT(queryManager->getQepStatus(parentPlanId) == QueryExecutionPlan::Running, "parent plan not running on net sink " << nesPartition);
            auto channel = networkManager->registerSubpartitionProducer(
                nodeLocation, nesPartition,
                waitTime, retryTimes);
            NES_ASSERT(channel, "Channel not valid partition "<< nesPartition);
            workerContext.storeChannel(nesPartition.getOperatorId(), std::move(channel));
            NES_DEBUG("NetworkSink: reconfigure() stored channel on " << nesPartition.toString() << " Thread " << NesThread::getId());
            break;
        }
        case Destroy: {
            workerContext.releaseChannel(nesPartition.getOperatorId());
            NES_DEBUG("NetworkSink: reconfigure() released channel on " << nesPartition.toString() << " Thread " << NesThread::getId());
            break;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("unsupported");
        }
    }
}

}// namespace Network

}// namespace NES
