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
#include <Monitoring/MetricValues/GroupedValues.hpp>
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

MonitoringManager::MonitoringManager(WorkerRPCClientPtr workerClient, TopologyPtr topology)
    : workerClient(workerClient), topology(topology) {
    NES_DEBUG("MonitoringManager: Init");
}

MonitoringManager::~MonitoringManager() {
    NES_DEBUG("MonitoringManager: Shutting down");
    workerClient.reset();
    topology.reset();
}

bool MonitoringManager::registerMonitoringPlan(std::vector<uint64_t> nodeIds, MonitoringPlanPtr monitoringPlan) {
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

            //TODO: add a grpc call to send the plan to the given node
            //workerClient->requestMonitoringData(destAddress, plan, tupleBuffer);

            monitoringPlanMap.emplace({nodeId, monitoringPlan});
        } else {
            NES_ERROR("MonitoringManager: Node with ID " + std::to_string(nodeId) + " does not exit.");
            return false;
        }
    }
    return true;
}

bool MonitoringManager::requestMonitoringData(uint64_t nodeId, TupleBuffer& tupleBuffer) {
    if (monitoringPlanMap.find(nodeId) == monitoringPlanMap.end()) {
        NES_ERROR("MonitoringManager: Retrieving metrics for " + std::to_string(nodeId) + " failed. No plan registered.");
        return false;
    }

    NES_DEBUG("MonitoringManager: Requesting metrics for node id= " + std::to_string(nodeId));
    TopologyNodePtr node = topology->findNodeWithId(nodeId);

    auto nodeIp = node->getIpAddress();
    auto nodeGrpcPort = node->getGrpcPort();
    std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);

    //TODO: add a grpc call to retrieve metrics of the given node
    //workerClient->requestMonitoringData(destAddress, plan, tupleBuffer);

    return true;
}

}// namespace NES