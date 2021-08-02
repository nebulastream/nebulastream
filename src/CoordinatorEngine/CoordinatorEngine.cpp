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

#include <API/Schema.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <CoordinatorEngine/CoordinatorEngine.hpp>
#include <CoordinatorRPCService.pb.h>
#include <NodeStats.pb.h>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

#include "TopologyManagerService.h"
#include "StreamCatalogService.h"


namespace NES {

CoordinatorEngine::CoordinatorEngine(StreamCatalogPtr streamCatalog, TopologyPtr topology)
    : streamCatalog(std::move(streamCatalog)), topology(std::move(topology)), streamCatalogService(streamCatalog), topologyManagerService(topology) {
    NES_DEBUG("CoordinatorEngine()");
}
CoordinatorEngine::~CoordinatorEngine() { NES_DEBUG("~CoordinatorEngine()"); };

uint64_t CoordinatorEngine::registerNode(const std::string& address,
                                         int64_t grpcPort,
                                         int64_t dataPort,
                                         uint16_t numberOfSlots,
                                         const NodeStatsPtr& nodeStats,
                                         NodeType type) {
    NES_TRACE("CoordinatorEngine: Register Node address=" << address << " numberOfSlots=" << numberOfSlots
                                                          << " nodeProperties=" << nodeStats->DebugString() << " type=" << type);
    std::unique_lock<std::mutex> lock(registerDeregisterNode);

    NES_DEBUG("CoordinatorEngine::registerNode: topology before insert");
    topology->print();

    if (topology->nodeExistsWithIpAndPort(address, grpcPort)) {
        NES_ERROR("CoordinatorEngine::registerNode: node with address " << address << " and grpc port " << grpcPort
                                                                        << " already exists");
        return false;
    }

    //get unique id for the new node
    uint64_t id;

    TopologyNodePtr physicalNode;
    if (type == NodeType::Sensor) {
        id = streamCatalogService.registerNode(address, grpcPort, dataPort, numberOfSlots, nodeStats);
    } else if (type == NodeType::Worker) {
        id = topologyManagerService.registerNode(address, grpcPort, dataPort, numberOfSlots, nodeStats);
    } else {
        NES_THROW_RUNTIME_ERROR("CoordinatorEngine::registerNode type not supported ");
    }

    NES_DEBUG("CoordinatorEngine::registerNode: topology after insert = ");
    topology->print();
    return id;
}

bool CoordinatorEngine::unregisterNode(uint64_t nodeId) {
    NES_DEBUG("CoordinatorEngine::UnregisterNode: try to disconnect sensor with id " << nodeId);
    std::unique_lock<std::mutex> lock(registerDeregisterNode);

    TopologyNodePtr physicalNode = topology->findNodeWithId(nodeId);

    if (!physicalNode) {
        NES_ERROR("CoordinatorActor: node with id not found " << nodeId);
        return false;
    }
    NES_DEBUG("CoordinatorEngine::UnregisterNode: found sensor, try to delete it in catalog");
    //remove from catalog
    bool successCatalog = streamCatalogService.unregisterNode(nodeId);
    NES_DEBUG("CoordinatorEngine::UnregisterNode: success in catalog is " << successCatalog);

    NES_DEBUG("CoordinatorEngine::UnregisterNode: found sensor, try to delete it in toplogy");
    //remove from topology
    bool successTopology = topologyManagerService.unregisterNode(nodeId);
    NES_DEBUG("CoordinatorEngine::UnregisterNode: success in topology is " << successTopology);

    return successCatalog && successTopology;
}

bool CoordinatorEngine::registerPhysicalStream(uint64_t nodeId,
                                               const std::string& sourceType,
                                               const std::string& physicalStreamName,
                                               const std::string& logicalStreamName) {
    TopologyNodePtr physicalNode = topology->findNodeWithId(nodeId);
    return streamCatalogService.registerPhysicalStream(physicalNode, sourceType, physicalStreamName, logicalStreamName);
}

bool CoordinatorEngine::unregisterPhysicalStream(uint64_t nodeId,
                                                 const std::string& physicalStreamName,
                                                 const std::string& logicalStreamName) {
    TopologyNodePtr physicalNode = topology->findNodeWithId(nodeId);
    return streamCatalogService.unregisterPhysicalStream(physicalNode, physicalStreamName, logicalStreamName);
}

bool CoordinatorEngine::registerLogicalStream(const std::string& logicalStreamName, const std::string& schemaString) {
    return streamCatalogService.registerLogicalStream(logicalStreamName, schemaString);
}

bool CoordinatorEngine::unregisterLogicalStream(const std::string& logicalStreamName) {
    return streamCatalogService.unregisterLogicalStream(logicalStreamName);
}

bool CoordinatorEngine::addParent(uint64_t childId, uint64_t parentId) {
    return topologyManagerService.addParent(childId, parentId);
}

bool CoordinatorEngine::removeParent(uint64_t childId, uint64_t parentId) {
    return topologyManagerService.removeParent(childId, parentId);
}

}// namespace NES