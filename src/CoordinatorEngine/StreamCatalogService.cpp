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
#include <CoordinatorEngine/StreamCatalogService.hpp>
#include <CoordinatorRPCService.pb.h>
#include <NodeStats.pb.h>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES {

StreamCatalogService::StreamCatalogService(StreamCatalogPtr streamCatalog)
    : streamCatalog(std::move(streamCatalog)) {
    NES_DEBUG("CoordinatorEngine()");
}
StreamCatalogService::~StreamCatalogService() { NES_DEBUG("~CoordinatorEngine()"); };


uint64_t StreamCatalogService::registerNode(const std::string& address,
                                         int64_t grpcPort,
                                         int64_t dataPort,
                                         uint16_t numberOfSlots,
                                         const NodeStatsPtr& nodeStats) {
    NES_TRACE("StreamCatalogService: Register Node address=" << address << " numberOfSlots=" << numberOfSlots
                                                          << " nodeProperties=" << nodeStats->DebugString());
    std::unique_lock<std::mutex> lock(registerDeregisterNode);

    //get unique id for the new node
    uint64_t id = UtilityFunctions::getNextTopologyNodeId();

    TopologyNodePtr physicalNode;

    NES_DEBUG("StreamCatalogService::registerNode: register sensor node");
    physicalNode = TopologyNode::create(id, address, grpcPort, dataPort, numberOfSlots);

    if (!physicalNode) {
        NES_ERROR("StreamCatalogService::RegisterNode : node not created");
        return 0;
    }

    //add default logical
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    //check if logical stream exists
    if (!streamCatalog->testIfLogicalStreamExistsInSchemaMapping(streamConf->getLogicalStreamName())) {
        NES_ERROR("StreamCatalogService::registerNode: error logical stream" << streamConf->getLogicalStreamName()
                                                                          << " does not exist when adding physical stream "
                                                                          << streamConf->getPhysicalStreamName());
        throw Exception("StreamCatalogService::registerNode logical stream does not exist "
                            + streamConf->getLogicalStreamName());
    }

    std::string sourceType = streamConf->getSourceType();
    if (sourceType != "CSVSource" && sourceType != "DefaultSource") {
        NES_ERROR("StreamCatalogService::registerNode: error source type " << sourceType << " is not supported");
        throw Exception("StreamCatalogService::registerNode: error source type " + sourceType + " is not supported");
    }

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);

    bool success = streamCatalog->addPhysicalStream(streamConf->getLogicalStreamName(), sce);
    if (!success) {
        NES_ERROR("StreamCatalogService::registerNode: physical stream " << streamConf->getPhysicalStreamName()
                                                                      << " could not be added to catalog");
        throw Exception("StreamCatalogService::registerNode: physical stream " + streamConf->getPhysicalStreamName()
                            + " could not be added to catalog");
    }

    return id;
}


bool StreamCatalogService::unregisterNode(uint64_t nodeId) {
    NES_DEBUG("StreamCatalogService::UnregisterNode: try to disconnect sensor with id " << nodeId);
    std::unique_lock<std::mutex> lock(registerDeregisterNode);

    NES_DEBUG("StreamCatalogService::UnregisterNode: found sensor, try to delete it in catalog");
    //remove from catalog
    bool successCatalog = streamCatalog->removePhysicalStreamByHashId(nodeId);
    NES_DEBUG("StreamCatalogService::UnregisterNode: success in catalog is " << successCatalog);

    return successCatalog;
}

bool StreamCatalogService::registerPhysicalStream(TopologyNodePtr physicalNode,
                                               const std::string& sourceType,
                                               const std::string& physicalStreamName,
                                               const std::string& logicalStreamName) {
    NES_DEBUG("StreamCatalogService::RegisterPhysicalStream: try to register physical node id "
                  << physicalNode->getId() << " physical stream=" << physicalStreamName << " logical stream=" << logicalStreamName);
    std::unique_lock<std::mutex> lock(addRemovePhysicalStream);

    //TopologyNodePtr physicalNode = topology->findNodeWithId(nodeId);
    if (!physicalNode) {
        NES_ERROR("StreamCatalogService::RegisterPhysicalStream node not found");
        return false;
    }
    StreamCatalogEntryPtr sce =
        std::make_shared<StreamCatalogEntry>(sourceType, physicalStreamName, logicalStreamName, physicalNode);
    bool success = streamCatalog->addPhysicalStream(logicalStreamName, sce);
    return success;
}

bool StreamCatalogService::unregisterPhysicalStream(TopologyNodePtr physicalNode,
                                                 const std::string& physicalStreamName,
                                                 const std::string& logicalStreamName) {
    NES_DEBUG("StreamCatalogService::UnregisterPhysicalStream: try to remove physical stream with name "
                  << physicalStreamName << " logical name " << logicalStreamName << " workerId=" << physicalNode->getId());
    std::unique_lock<std::mutex> lock(addRemovePhysicalStream);

    if (!physicalNode) {
        NES_DEBUG("StreamCatalogService::UnregisterPhysicalStream: sensor not found with workerId" << physicalNode->getId());
        return false;
    }
    NES_DEBUG("CoordinatorEngine: node=" << physicalNode->toString());

    bool success = streamCatalog->removePhysicalStream(logicalStreamName, physicalStreamName, physicalNode->getId());
    return success;
}

bool StreamCatalogService::registerLogicalStream(const std::string& logicalStreamName, const std::string& schemaString) {
    NES_DEBUG("StreamCatalogService::registerLogicalStream: register logical stream=" << logicalStreamName
                                                                                   << " schema=" << schemaString);
    std::unique_lock<std::mutex> lock(addRemoveLogicalStream);
    return streamCatalog->addLogicalStream(logicalStreamName, schemaString);
}

bool StreamCatalogService::unregisterLogicalStream(const std::string& logicalStreamName) {
    std::unique_lock<std::mutex> lock(addRemoveLogicalStream);
    NES_DEBUG("StreamCatalogService::unregisterLogicalStream: register logical stream=" << logicalStreamName);
    bool success = streamCatalog->removeLogicalStream(logicalStreamName);
    return success;
}

}//namespace NES

