/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Services/MonitoringService.hpp>

#include <GRPC/WorkerRPCClient.hpp>
#include <Monitoring/MetricValues/MetricValueType.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/http_client.h>

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

std::tuple<SchemaPtr, TupleBuffer> MonitoringService::requestMonitoringData(const std::string& ipAddress, int64_t grpcPort,
                                                                            MonitoringPlanPtr plan) {
    if (!plan) {
        auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
        plan = MonitoringPlan::create(metrics);
    }
    std::string destAddress = ipAddress + ":" + std::to_string(grpcPort);
    NES_DEBUG("NesCoordinator: Requesting monitoring data from worker address= " + destAddress);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto schema = workerClient->requestMonitoringData(destAddress, plan, tupleBuffer);

    return std::make_tuple(schema, tupleBuffer);
}

web::json::value MonitoringService::requestMonitoringDataAsJson(const std::string& ipAddress, int64_t grpcPort,
                                                                MonitoringPlanPtr plan) {
    auto [schema, tupleBuffer] = requestMonitoringData(ipAddress, grpcPort, plan);
    web::json::value metricsJson{};
    metricsJson["schema"] = web::json::value::string(schema->toString());
    metricsJson["tupleBuffer"] = web::json::value::string(UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, schema));
    tupleBuffer.release();
    schema.reset();
    return metricsJson;
}

web::json::value MonitoringService::requestMonitoringDataAsJson(int64_t nodeId, MonitoringPlanPtr plan) {
    if (!plan) {
        auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
        plan = MonitoringPlan::create(metrics);
    }
    NES_DEBUG("NesCoordinator: Requesting monitoring data from worker id= " + std::to_string(nodeId));
    TopologyNodePtr node = topology->findNodeWithId(nodeId);

    if (node) {
        auto nodeIp = node->getIpAddress();
        auto nodeGrpcPort = node->getGrpcPort();
        return requestMonitoringDataAsJson(nodeIp, nodeGrpcPort, plan);
    } else {
        throw std::runtime_error("MonitoringService: Node with ID " + std::to_string(nodeId) + " does not exit.");
    }
}

web::json::value MonitoringService::requestMonitoringDataForAllNodesAsJson(MonitoringPlanPtr plan) {
    web::json::value metricsJson{};
    auto root = topology->getRoot();
    metricsJson[std::to_string(root->getId())] = requestMonitoringDataAsJson(root->getId(), plan);

    for (const auto& node : root->getAndFlattenAllChildren(false)) {
        std::shared_ptr<TopologyNode> tNode = node->as<TopologyNode>();
        metricsJson[std::to_string(tNode->getId())] = requestMonitoringDataAsJson(tNode->getId(), plan);
    }
    return metricsJson;
}

utf8string MonitoringService::requestMonitoringDataViaPrometheusAsString(int64_t nodeId, int16_t port, MonitoringPlanPtr plan) {
    if (!plan) {
        auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
        plan = MonitoringPlan::create(metrics);
    }
    NES_DEBUG("NesCoordinator: Requesting monitoring data from worker id= " + std::to_string(nodeId));
    TopologyNodePtr node = topology->findNodeWithId(nodeId);

    if (node) {
        auto nodeIp = node->getIpAddress();
        auto address = "http://" + nodeIp + ":" + std::to_string(port) + "/metrics";

        utf8string metricsReturn;
        web::http::client::http_client clientQ1(address);
        NES_INFO("MonitoringController: Executing metric request to prometheus node exporter on " + address);
        clientQ1.request(web::http::methods::GET)
            .then([](const web::http::http_response& response) {
                NES_INFO("MonitoringController: Extract metrics from prometheus node exporter response.");
                return response.extract_utf8string();
            })
            .then([&metricsReturn](const pplx::task<utf8string>& task) {
                NES_INFO("MonitoringController: Set metrics return from node exporter responses.");
                metricsReturn = task.get();
            })
            .wait();
        return metricsReturn;
    } else {
        throw std::runtime_error("MonitoringService: Node with ID " + std::to_string(nodeId) + " does not exit.");
    }
}

web::json::value MonitoringService::requestMonitoringDataFromAllNodesViaPrometheusAsJson(MonitoringPlanPtr plan) {
    web::json::value metricsJson{};
    auto root = topology->getRoot();
    metricsJson[std::to_string(root->getId())] =
        web::json::value::string(requestMonitoringDataViaPrometheusAsString(root->getId(), 9100, plan));

    for (const auto& node : root->getAndFlattenAllChildren(false)) {
        std::shared_ptr<TopologyNode> tNode = node->as<TopologyNode>();
        metricsJson[std::to_string(tNode->getId())] =
            web::json::value::string(requestMonitoringDataViaPrometheusAsString(root->getId(), 9100, plan));
    }
    return metricsJson;
}

}// namespace NES