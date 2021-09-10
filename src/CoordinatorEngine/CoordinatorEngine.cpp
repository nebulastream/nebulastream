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
#include <Services/StreamCatalogService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES {

CoordinatorEngine::CoordinatorEngine(StreamCatalogPtr streamCatalogPtr, TopologyPtr topologyPtr)
    : streamCatalogService(streamCatalogPtr), topologyManagerService(topologyPtr, streamCatalogPtr),
      streamCatalog(std::move(streamCatalogPtr)), topology(std::move(topologyPtr)) {
    NES_DEBUG("ServiceTests()");
}
CoordinatorEngine::~CoordinatorEngine() { NES_DEBUG("~ServiceTests()"); };

uint64_t CoordinatorEngine::registerNode(const std::string& address,
                                         int64_t grpcPort,
                                         int64_t dataPort,
                                         uint16_t numberOfSlots,
                                         const NodeStatsPtr& nodeStats,
                                         NodeType type) {
    return topologyManagerService.registerNode(address, grpcPort, dataPort, numberOfSlots, nodeStats, type);
}

bool CoordinatorEngine::unregisterNode(uint64_t nodeId) { return topologyManagerService.unregisterNode(nodeId); }

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