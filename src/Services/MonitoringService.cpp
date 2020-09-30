#include <Services/MonitoringService.hpp>

#include <GRPC/WorkerRPCClient.hpp>
#include <Monitoring/MetricValues/MetricValueType.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Topology/Topology.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

MonitoringService::MonitoringService(WorkerRPCClientPtr workerClient, TopologyPtr topology, BufferManagerPtr bufferManager)
    : workerClient(workerClient), topology(topology), bufferManager(bufferManager) {
    NES_DEBUG("MonitoringService: Initializing");
}

MonitoringService::~MonitoringService() {
    NES_DEBUG("MonitoringService: Shutting down");
    workerClient.reset();
    topology.reset();
}

web::json::value MonitoringService::getTopologyAsJson() const {
    return UtilityFunctions::getTopologyAsJson(topology->getRoot());
}

web::json::value MonitoringService::requestMonitoringData(const std::string& ipAddress, int64_t grpcPort, MonitoringPlanPtr plan) {
    if (!plan) {
        auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
        plan = MonitoringPlan::create(metrics);
    }
    std::string destAddress = ipAddress + ":" + std::to_string(grpcPort);
    NES_DEBUG("NesCoordinator: Requesting monitoring data from worker address= " + destAddress);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto schema = workerClient->requestMonitoringData(destAddress, plan, tupleBuffer);

    web::json::value metricsJson{};
    metricsJson["schema"] = web::json::value::string(schema->toString());
    metricsJson["tupleBuffer"] = web::json::value::string(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));
    tupleBuffer.release();
    return metricsJson;
}

web::json::value MonitoringService::requestMonitoringData(int64_t nodeId, MonitoringPlanPtr plan) {
    if (!plan) {
        auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
        plan = MonitoringPlan::create(metrics);
    }
    NES_DEBUG("NesCoordinator: Requesting monitoring data from worker id= " + std::to_string(nodeId));
    auto node = topology->findNodeWithId(nodeId);

    if (node) {
        auto nodeIp = node->getIpAddress();
        auto nodeGrpcPort = node->getGrpcPort();
        return requestMonitoringData(nodeIp, nodeGrpcPort, plan);
    }
    else {
        throw std::runtime_error("MonitoringService: Node with ID " + std::to_string(nodeId) + " does not exit.");
    }
}

web::json::value MonitoringService::requestMonitoringDataForAllNodes(MonitoringPlanPtr plan) {
    web::json::value metricsJson{};
    auto root = topology->getRoot();
    metricsJson[std::to_string(root->getId())] = requestMonitoringData(root->getId(), plan);

    for (const auto& node: root->getAndFlattenAllChildren()) {
        std::shared_ptr<TopologyNode> tNode = std::dynamic_pointer_cast<TopologyNode>(node);
        metricsJson[std::to_string(tNode->getId())] = requestMonitoringData(tNode->getId(), plan);
    }
    return metricsJson;
}

}// namespace NES