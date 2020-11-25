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
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

CoordinatorEngine::CoordinatorEngine(StreamCatalogPtr streamCatalog, TopologyPtr topology)
    : streamCatalog(streamCatalog), topology(topology), registerDeregisterNode(), addRemoveLogicalStream(),
      addRemovePhysicalStream() {
    NES_DEBUG("CoordinatorEngine()");
}
CoordinatorEngine::~CoordinatorEngine() { NES_DEBUG("~CoordinatorEngine()"); };

uint64_t CoordinatorEngine::registerNode(std::string address, int64_t grpcPort, int64_t dataPort, uint16_t numberOfSlots,
                                         NodeStats nodeStats, NodeType type) {
    NES_TRACE("CoordinatorEngine: Register Node address=" << address << " numberOfSlots=" << numberOfSlots
                                                          << " nodeProperties=" << nodeStats.DebugString() << " type=" << type);
    std::unique_lock<std::mutex> lock(registerDeregisterNode);

    NES_DEBUG("CoordinatorEngine::registerNode: topology before insert");
    topology->print();

    if (topology->nodeExistsWithIpAndPort(address, grpcPort)) {
        NES_ERROR("CoordinatorEngine::registerNode: node with address " << address << " and grpc port " << grpcPort
                                                                        << " already exists");
        return false;
    }

    //get unique id for the new node
    uint64_t id = UtilityFunctions::getNextNodeId();

    TopologyNodePtr physicalNode;
    if (type == NodeType::Sensor) {
        NES_DEBUG("CoordinatorEngine::registerNode: register sensor node");
        physicalNode = TopologyNode::create(id, address, grpcPort, dataPort, numberOfSlots);

        if (!physicalNode) {
            NES_ERROR("CoordinatorEngine::RegisterNode : node not created");
            return 0;
        }

        //add default logical
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
        //check if logical stream exists
        if (!streamCatalog->testIfLogicalStreamExistsInSchemaMapping(streamConf->getLogicalStreamName())) {
            NES_ERROR("CoordinatorEngine::registerNode: error logical stream" << streamConf->getLogicalStreamName()
                                                                              << " does not exist when adding physical stream "
                                                                              << streamConf->getPhysicalStreamName());
            throw Exception("CoordinatorEngine::registerNode logical stream does not exist "
                            + streamConf->getLogicalStreamName());
        }

        SchemaPtr schema = streamCatalog->getSchemaForLogicalStream(streamConf->getLogicalStreamName());

        std::string sourceType = streamConf->getSourceType();
        if (sourceType != "CSVSource" && sourceType != "DefaultSource") {
            NES_ERROR("CoordinatorEngine::registerNode: error source type " << sourceType << " is not supported");
            throw Exception("CoordinatorEngine::registerNode: error source type " + sourceType + " is not supported");
        }

        StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);

        bool success = streamCatalog->addPhysicalStream(streamConf->getLogicalStreamName(), sce);
        if (!success) {
            NES_ERROR("CoordinatorEngine::registerNode: physical stream " << streamConf->getPhysicalStreamName()
                                                                          << " could not be added to catalog");
            throw Exception("CoordinatorEngine::registerNode: physical stream " + streamConf->getPhysicalStreamName()
                            + " could not be added to catalog");
        }

    } else if (type == NodeType::Worker) {
        NES_DEBUG("CoordinatorEngine::registerNode: register worker node");
        physicalNode = TopologyNode::create(id, address, grpcPort, dataPort, numberOfSlots);

        if (!physicalNode) {
            NES_ERROR("CoordinatorEngine::RegisterNode : node not created");
            return 0;
        }
    } else {
        NES_THROW_RUNTIME_ERROR("CoordinatorEngine::registerNode type not supported ");
    }

    if (nodeStats.IsInitialized()) {
        physicalNode->setNodeStats(nodeStats);
    }

    const TopologyNodePtr rootNode = topology->getRoot();

    if (!rootNode) {
        NES_DEBUG("CoordinatorEngine::registerNode: tree is empty so this becomes new root");
        topology->setAsRoot(physicalNode);
    } else {
        NES_DEBUG("CoordinatorEngine::registerNode: add link to the root node " << rootNode->toString());
        topology->addNewPhysicalNodeAsChild(rootNode, physicalNode);
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
    bool successCatalog = streamCatalog->removePhysicalStreamByHashId(nodeId);
    NES_DEBUG("CoordinatorEngine::UnregisterNode: success in catalog is " << successCatalog);

    NES_DEBUG("CoordinatorEngine::UnregisterNode: found sensor, try to delete it in toplogy");
    //remove from topology
    bool successTopology = topology->removePhysicalNode(physicalNode);
    NES_DEBUG("CoordinatorEngine::UnregisterNode: success in topology is " << successTopology);

    return successCatalog && successTopology;
}

bool CoordinatorEngine::registerPhysicalStream(uint64_t nodeId, std::string sourceType, std::string sourceConf,
                                               uint64_t sourceFrequency, uint64_t numberOfTuplesToProducePerBuffer,
                                               uint64_t numberOfBuffersToProduce, std::string physicalStreamname,
                                               std::string logicalStreamname) {
    NES_DEBUG("CoordinatorEngine::RegisterPhysicalStream: try to register physical node id "
              << nodeId << " physical stream=" << physicalStreamname << " logical stream=" << logicalStreamname);
    std::unique_lock<std::mutex> lock(addRemovePhysicalStream);
    PhysicalStreamConfigPtr streamConf =
        PhysicalStreamConfig::create(sourceType, sourceConf, sourceFrequency, numberOfTuplesToProducePerBuffer,
                                     numberOfBuffersToProduce, physicalStreamname, logicalStreamname);
    NES_DEBUG("CoordinatorEngine::RegisterPhysicalStream: try to register physical stream with conf= "
              << streamConf->toString() << " for workerId=" << nodeId);
    TopologyNodePtr physicalNode = topology->findNodeWithId(nodeId);
    if (!physicalNode) {
        NES_ERROR("CoordinatorEngine::RegisterPhysicalStream node not found");
        return false;
    }
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);
    bool success = streamCatalog->addPhysicalStream(streamConf->getLogicalStreamName(), sce);
    return success;
}

bool CoordinatorEngine::unregisterPhysicalStream(uint64_t nodeId, std::string physicalStreamName, std::string logicalStreamName) {
    NES_DEBUG("CoordinatorEngine::UnregisterPhysicalStream: try to remove physical stream with name "
              << physicalStreamName << " logical name " << logicalStreamName << " workerId=" << nodeId);
    std::unique_lock<std::mutex> lock(addRemovePhysicalStream);

    TopologyNodePtr physicalNode = topology->findNodeWithId(nodeId);
    if (!physicalNode) {
        NES_DEBUG("CoordinatorEngine::UnregisterPhysicalStream: sensor not found with workerId" << nodeId);
        return false;
    }
    NES_DEBUG("CoordinatorEngine: node=" << physicalNode->toString());

    bool success = streamCatalog->removePhysicalStream(logicalStreamName, physicalStreamName, nodeId);
    return success;
}

bool CoordinatorEngine::registerLogicalStream(std::string logicalStreamName, std::string schemaString) {
    NES_DEBUG("CoordinatorEngine::registerLogicalStream: register logical stream=" << logicalStreamName
                                                                                   << " schema=" << schemaString);
    std::unique_lock<std::mutex> lock(addRemoveLogicalStream);

    SchemaPtr schema = UtilityFunctions::createSchemaFromCode(schemaString);
    NES_DEBUG("StreamCatalogService: schema successfully created");
    bool success = streamCatalog->addLogicalStream(logicalStreamName, schema);
    return success;
}

bool CoordinatorEngine::unregisterLogicalStream(std::string logicalStreamName) {
    std::unique_lock<std::mutex> lock(addRemoveLogicalStream);
    NES_DEBUG("CoordinatorEngine::unregisterLogicalStream: register logical stream=" << logicalStreamName);
    bool success = streamCatalog->removeLogicalStream(logicalStreamName);
    return success;
}

bool CoordinatorEngine::addParent(uint64_t childId, uint64_t parentId) {
    NES_DEBUG("CoordinatorEngine::addParent: childId=" << childId << " parentId=" << parentId);

    if (childId == parentId) {
        NES_ERROR("CoordinatorEngine::AddParent: cannot add link to itself");
        return false;
    }

    TopologyNodePtr childPhysicalNode = topology->findNodeWithId(childId);
    if (!childPhysicalNode) {
        NES_ERROR("CoordinatorEngine::AddParent: source node " << childId << " does not exists");
        return false;
    }
    NES_DEBUG("CoordinatorEngine::AddParent: source node " << childId << " exists");

    TopologyNodePtr parentPhysicalNode = topology->findNodeWithId(parentId);
    if (!parentPhysicalNode) {
        NES_ERROR("CoordinatorEngine::AddParent: sensorParent node " << parentId << " does not exists");
        return false;
    }
    NES_DEBUG("CoordinatorEngine::AddParent: sensorParent node " << parentId << " exists");

    for (auto& child : parentPhysicalNode->getChildren()) {
        if (child->as<TopologyNode>()->getId() == childId) {
            NES_ERROR("CoordinatorEngine::AddParent: nodes " << childId << " and " << parentId << " already exists");
            return false;
        }
    }

    bool added = topology->addNewPhysicalNodeAsChild(parentPhysicalNode, childPhysicalNode);
    if (added) {
        NES_DEBUG("CoordinatorEngine::AddParent: created link successfully");
        return true;
    } else {
        NES_ERROR("CoordinatorEngine::AddParent: created NOT successfully added");
        return false;
    }
}

bool CoordinatorEngine::removeParent(uint64_t childId, uint64_t parentId) {
    NES_DEBUG("CoordinatorEngine::removeParent: childId=" << childId << " parentId=" << parentId);

    TopologyNodePtr childNode = topology->findNodeWithId(childId);
    if (!childNode) {
        NES_ERROR("CoordinatorEngine::removeParent: source node " << childId << " does not exists");
        return false;
    }
    NES_DEBUG("CoordinatorEngine::removeParent: source node " << childId << " exists");

    TopologyNodePtr parentNode = topology->findNodeWithId(parentId);
    if (!parentNode) {
        NES_ERROR("CoordinatorEngine::removeParent: sensorParent node " << childId << " does not exists");
        return false;
    }

    NES_DEBUG("CoordinatorEngine::AddParent: sensorParent node " << parentId << " exists");

    std::vector<NodePtr> children = parentNode->getChildren();
    auto found = std::find_if(children.begin(), children.end(), [&](NodePtr node) {
        return node->as<TopologyNode>()->getId() == childId;
    });

    if (found == children.end()) {
        NES_ERROR("CoordinatorEngine::removeParent: nodes " << childId << " and " << parentId << " are not connected");
        return false;
    }

    for (auto& child : children) {
        if (child->as<TopologyNode>()->getId() == childId) {
        }
    }

    NES_DEBUG("CoordinatorEngine::removeParent: nodes connected");

    bool success = topology->removeNodeAsChild(parentNode, childNode);
    if (!success) {
        NES_ERROR("CoordinatorEngine::removeParent: edge between  " << childId << " and " << parentId << " could not be removed");
        return false;
    }
    NES_DEBUG("CoordinatorEngine::removeParent: successful");
    return true;
}
}// namespace NES