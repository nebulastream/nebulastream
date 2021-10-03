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

#include <Monitoring/MonitoringManager.hpp>
#include <Util/Logger.hpp>

#include <API/Schema.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Monitoring/MetricStore.hpp>
#include <Monitoring/MetricValues/GroupedMetricValues.hpp>
#include <Monitoring/MetricValues/MetricValueType.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Runtime/BufferManager.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/http_client.h>
#include <utility>

namespace NES {

MonitoringManager::MonitoringManager(WorkerRPCClientPtr workerClient, TopologyPtr topology)
    : metricStore(std::make_shared<MetricStore>(MetricStoreType::NEWEST)), workerClient(workerClient), topology(topology) {
    NES_DEBUG("MonitoringManager: Init");
}

MonitoringManager::~MonitoringManager() {
    NES_DEBUG("MonitoringManager: Shutting down");
    workerClient.reset();
    topology.reset();
}

bool MonitoringManager::registerRemoteMonitoringPlans(const std::vector<uint64_t>& nodeIds,
                                                      const MonitoringPlanPtr& monitoringPlan) {
    if (!monitoringPlan) {
        NES_ERROR("MonitoringManager: Register monitoring plan failed, no plan is provided.");
        return false;
    }
    if (nodeIds.empty()) {
        NES_ERROR("MonitoringManager: Register monitoring plan failed, no nodes are provided.");
        return false;
    }

    for (auto nodeId : nodeIds) {
        NES_DEBUG("MonitoringManager: Registering monitoring plan for worker id= " + std::to_string(nodeId));
        TopologyNodePtr node = topology->findNodeWithId(nodeId);

        if (node) {
            auto nodeIp = node->getIpAddress();
            auto nodeGrpcPort = node->getGrpcPort();
            std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);

            auto success = workerClient->registerMonitoringPlan(destAddress, monitoringPlan);

            if (success) {
                NES_DEBUG("MonitoringManager: Node with ID " + std::to_string(nodeId) + " registered successfully.");
                monitoringPlanMap[nodeId] = monitoringPlan;
            } else {
                NES_ERROR("MonitoringManager: Node with ID " + std::to_string(nodeId) + " failed to register plan over GRPC.");
                return false;
            }
        } else {
            NES_ERROR("MonitoringManager: Node with ID " + std::to_string(nodeId) + " does not exit.");
            return false;
        }
    }
    return true;
}

GroupedMetricValues MonitoringManager::requestMonitoringData(uint64_t nodeId, Runtime::BufferManagerPtr bufferManager) {
    NES_DEBUG("MonitoringManager: Requesting metrics for node id=" + std::to_string(nodeId));
    auto plan = getMonitoringPlan(nodeId);
    auto schema = plan->createSchema();

    //getMonitoringPlan(..) checks if node exists, so no further check necessary
    TopologyNodePtr node = topology->findNodeWithId(nodeId);
    auto tupleBuffer = bufferManager->getUnpooledBuffer(schema->getSchemaSizeInBytes()).value();

    auto nodeIp = node->getIpAddress();
    auto nodeGrpcPort = node->getGrpcPort();
    std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);
    auto success = workerClient->requestMonitoringData(destAddress, tupleBuffer, schema->getSchemaSizeInBytes());

    if (success) {
        NES_DEBUG("MonitoringManager: Received monitoring data with status " + std::to_string(success));
        GroupedMetricValues parsedValues = plan->fromBuffer(schema, tupleBuffer);
        return parsedValues;
    }
    NES_THROW_RUNTIME_ERROR("MonitoringManager: Error receiving monitoring metrics for node with id "
                            + std::to_string(node->getId()));
}

void MonitoringManager::receiveMonitoringData(uint64_t nodeId, GroupedMetricValuesPtr metrics) {
    metricStore->addMetric(nodeId, std::move(metrics));
}

GroupedMetricValuesPtr MonitoringManager::requestNewestMonitoringDataFromMetricStore(uint64_t nodeId) {
    if (metricStore->hasMetric(nodeId)) {
        return metricStore->getNewestMetric(nodeId);
    }
    else {
        NES_THROW_RUNTIME_ERROR("MonitoringManager: Node with ID " << nodeId << " not found in MetricStore.");
    }
}


MonitoringPlanPtr MonitoringManager::getMonitoringPlan(uint64_t nodeId) {
    if (monitoringPlanMap.find(nodeId) == monitoringPlanMap.end()) {
        TopologyNodePtr node = topology->findNodeWithId(nodeId);
        if (node) {
            NES_DEBUG("MonitoringManager: No registered plan found. Returning default plan for node " + std::to_string(nodeId));
            return MonitoringPlan::DefaultPlan();
        }
        NES_THROW_RUNTIME_ERROR("MonitoringManager: Retrieving metrics for " + std::to_string(nodeId)
                                + " failed. Node does not exist in topology.");

    } else {
        return monitoringPlanMap[nodeId];
    }
}

}// namespace NES