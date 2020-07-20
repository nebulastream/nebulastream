#include <API/Schema.hpp>
#include <CoordinatorEngine/CoordinatorEngine.hpp>
#include <Topology/TopologyManager.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

CoordinatorEngine::CoordinatorEngine(StreamCatalogPtr streamCatalog, TopologyManagerPtr topologyManager)
    : streamCatalog(streamCatalog),
      topologyManager(topologyManager),
      registerDeregisterNode(),
      addRemoveLogicalStream(),
      addRemovePhysicalStream() {
    NES_DEBUG("CoordinatorEngine()");
}

size_t getIdFromIp(std::string ip) {
    std::hash<std::string> hashFn;
    string uidString = UtilityFunctions::generateIdString();
    return hashFn(uidString + ip);
}

size_t CoordinatorEngine::registerNode(std::string address,
                                       size_t numberOfCPUs,
                                       std::string nodeProperties,
                                       NESNodeType type) {
    NES_DEBUG("CoordinatorEngine: Register Node address=" << address
                                                          << " numberOfCpus=" << numberOfCPUs
                                                          << " nodeProperties=" << nodeProperties
                                                          << " type=" << type);
    std::unique_lock<std::mutex> lock(registerDeregisterNode);

    NES_DEBUG("CoordinatorEngine::registerNode: topology before insert");
    topologyManager->printNESTopologyPlan();

    //get unique id for the new node
    size_t id = getIdFromIp(address);

    auto nodes = topologyManager->getNESTopologyPlan()->getNodeByIp(address);
    if (!nodes.empty()) {
        NES_ERROR("CoordinatorEngine::registerNode: node with this address already exists=" << address << " id="
                                                                                            << nodes[0]->getId());
        return false;
    }
    NESTopologyEntryPtr nodePtr;
    if (type == NESNodeType::Sensor) {
        NES_DEBUG("CoordinatorEngine::registerNode: register sensor node");
        nodePtr = topologyManager->createNESSensorNode(id,
                                                       address,
                                                       CPUCapacity::Value(numberOfCPUs));

        if (!nodePtr) {
            NES_ERROR("CoordinatorEngine::RegisterNode : node not created");
            return 0;
        }

        PhysicalStreamConfig streamConf;
        NESTopologySensorNodePtr sensor = std::dynamic_pointer_cast<NESTopologySensorNode>(nodePtr);
        sensor->setPhysicalStreamName(streamConf.physicalStreamName);

        NES_DEBUG(
            "CoordinatorEngine::registerNode: try to register sensor phyName=" << streamConf.physicalStreamName
                                                                               << " logName="
                                                                               << streamConf.logicalStreamName
                                                                               << " nodeID=" << nodePtr->getId());

        //check if logical stream exists
        if (!streamCatalog->testIfLogicalStreamExistsInSchemaMapping(
                streamConf.logicalStreamName)) {
            NES_ERROR(
                "CoordinatorEngine::registerNode: error logical stream" << streamConf.logicalStreamName
                                                                        << " does not exist when adding physical stream "
                                                                        << streamConf.physicalStreamName);
            throw Exception(
                "CoordinatorEngine::registerNode logical stream does not exist " + streamConf.logicalStreamName);
        }

        SchemaPtr schema = streamCatalog->getSchemaForLogicalStream(
            streamConf.logicalStreamName);

        if (streamConf.sourceType != "CSVSource"
            && streamConf.sourceType != "DefaultSource") {
            NES_ERROR(
                "CoordinatorEngine::registerNode: error source type " << streamConf.sourceType
                                                                      << " is not supported");
            throw Exception(
                "CoordinatorEngine::registerNode: error source type " + streamConf.sourceType
                + " is not supported");
        }

        StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
            streamConf, nodePtr);

        bool success = streamCatalog->addPhysicalStream(
            streamConf.logicalStreamName, sce);
        if (!success) {
            NES_ERROR(
                "CoordinatorEngine::registerNode: physical stream " << streamConf.physicalStreamName
                                                                    << " could not be added to catalog");
            throw Exception(
                "CoordinatorEngine::registerNode: physical stream " + streamConf.physicalStreamName
                + " could not be added to catalog");
        }

    } else if (type == NESNodeType::Worker) {
        NES_DEBUG("CoordinatorEngine::registerNode: register worker node");
        nodePtr = topologyManager->createNESWorkerNode(id, address, CPUCapacity::Value(numberOfCPUs));

        if (!nodePtr) {
            NES_ERROR("CoordinatorEngine::RegisterNode : node not created");
            return 0;
        }
    } else {
        NES_THROW_RUNTIME_ERROR("CoordinatorEngine::registerNode type not supported ");
    }
    assert(nodePtr);

    if (nodeProperties != "defaultProperties") {
        nodePtr->setNodeProperty(nodeProperties);
    }

    const NESTopologyEntryPtr kRootNode = topologyManager->getRootNode();

    if (!kRootNode) {
        NES_DEBUG("CoordinatorEngine::registerNode: tree is empty so this becomes new root");
    } else {
        NES_DEBUG("CoordinatorEngine::registerNode: add link to root node " << kRootNode << " of type"
                                                                            << kRootNode->getEntryType());
        topologyManager->createNESTopologyLink(nodePtr, kRootNode, 1, 1);
    }

    NES_DEBUG("CoordinatorEngine::registerNode: topology after insert = "
              << topologyManager->getNESTopologyPlan()->getTopologyPlanString());

    return id;
}

bool CoordinatorEngine::unregisterNode(size_t nodeId) {
    NES_DEBUG("CoordinatorEngine::UnregisterNode: try to disconnect sensor with id " << nodeId);
    std::unique_lock<std::mutex> lock(registerDeregisterNode);

    std::vector<NESTopologyEntryPtr> sensorNodes =
        topologyManager->getNESTopologyPlan()->getNodeById(
            nodeId);

    size_t numberOfOccurrences = 0;

    NESTopologyEntryPtr sensorNode;
    for (auto e : sensorNodes) {
        if (e->getEntryType() != Coordinator) {
            sensorNode = e;
            numberOfOccurrences++;
        }
    }
    if (numberOfOccurrences > 1) {
        NES_ERROR(
            "CoordinatorEngine::UnregisterNode: more than one worker node found with id " << nodeId
                                                                                          << " numberOfOccurrences="
                                                                                          << numberOfOccurrences);
        return false;
    } else if (numberOfOccurrences == 0) {
        NES_ERROR("CoordinatorActor: node with id not found " << nodeId);
        return false;
    }
    NES_DEBUG("CoordinatorEngine::UnregisterNode: found sensor, try to delete it in catalog");
    //remove from catalog
    bool successCatalog = streamCatalog->removePhysicalStreamByHashId(
        nodeId);
    NES_DEBUG("CoordinatorEngine::UnregisterNode: success in catalog is " << successCatalog);

    NES_DEBUG("CoordinatorEngine::UnregisterNode: found sensor, try to delete it in toplogy");
    //remove from topology
    bool successTopology = topologyManager->removeNESNode(sensorNode);
    NES_DEBUG("CoordinatorEngine::UnregisterNode: success in topology is " << successTopology);

    return successCatalog && successTopology;
}

bool CoordinatorEngine::registerPhysicalStream(size_t nodeId,
                                               std::string sourcetype,
                                               std::string sourceconf,
                                               size_t sourcefrequency,
                                               size_t numberofbufferstoproduce,
                                               std::string physicalstreamname,
                                               std::string logicalstreamname) {
    NES_DEBUG("CoordinatorEngine::RegisterPhysicalStream: try to register physical node id " << nodeId << " physical stream=" << physicalstreamname
              << " logical stream=" << logicalstreamname);
    std::unique_lock<std::mutex> lock(addRemovePhysicalStream);

    PhysicalStreamConfig streamConf(sourcetype,
                                    sourceconf,
                                    sourcefrequency,
                                    numberofbufferstoproduce,
                                    physicalstreamname,
                                    logicalstreamname);

    NES_DEBUG(
        "CoordinatorEngine::RegisterPhysicalStream: try to register physical stream with conf= "
        << streamConf.toString() << " for workerId="
        << nodeId);

    std::vector<NESTopologyEntryPtr> sensorNodes =
        topologyManager->getNESTopologyPlan()->getNodeById(
            nodeId);

    if (sensorNodes.size() == 0) {
        NES_ERROR("CoordinatorEngine::RegisterPhysicalStream node not found");
        return false;
    }
    if (sensorNodes.size() > 1) {
        NES_ERROR("CoordinatorEngine::RegisterPhysicalStream more than one node found");
        return false;
    }

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
        streamConf, sensorNodes[0]);

    bool success = streamCatalog->addPhysicalStream(
        streamConf.logicalStreamName, sce);

    return success;
}

bool CoordinatorEngine::unregisterPhysicalStream(size_t nodeId, std::string physicalStreamName,
                                                 std::string logicalStreamName) {
    NES_DEBUG(
        "CoordinatorEngine::UnregisterPhysicalStream: try to remove physical "
        "stream with name "
        << physicalStreamName << " logical name "
        << logicalStreamName << " workerId=" << nodeId);
    std::unique_lock<std::mutex> lock(addRemovePhysicalStream);

    std::vector<NESTopologyEntryPtr> sensorNodes =
        topologyManager->getNESTopologyPlan()->getNodeById(
            nodeId);

    //TODO: note that we remove on the first node with this id
    NESTopologyEntryPtr sensorNode;
    for (auto e : sensorNodes) {
        if (e->getEntryType() != Coordinator) {
            sensorNode = e;
            break;
        }
    }

    if (sensorNode == nullptr) {
        NES_DEBUG(
            "CoordinatorEngine::UnregisterPhysicalStream: sensor not found with workerId" << nodeId);
        return false;
    }
    NES_DEBUG("node type=" << sensorNode->getEntryTypeString());

    bool success = streamCatalog->removePhysicalStream(logicalStreamName,
                                                       physicalStreamName,
                                                       nodeId);
    return success;
}

bool CoordinatorEngine::registerLogicalStream(std::string logicalStreamName, std::string schemaString) {
    NES_DEBUG("CoordinatorEngine::registerLogicalStream: register logical stream=" << logicalStreamName << " schema="
                                                                                   << schemaString);
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

bool CoordinatorEngine::addParent(size_t childId, size_t parentId) {
    NES_DEBUG("CoordinatorEngine::addParent: childId=" << childId << " parentId=" << parentId);

    if (childId == parentId) {
        NES_ERROR("CoordinatorEngine::AddParent: cannot add link to itself");
        return false;
    }

    std::vector<NESTopologyEntryPtr>
        sensorNodes = topologyManager->getNESTopologyPlan()->getNodeById(childId);
    if (sensorNodes.size() == 0) {
        NES_ERROR("CoordinatorEngine::AddParent: source node " << childId << " does not exists");
        return false;
    } else if (sensorNodes.size() > 1) {
        NES_ERROR(
            "CoordinatorEngine::AddParent: more than one sensorNodes node with id exists " << childId
                                                                                           << " count="
                                                                                           << sensorNodes.size());
        return false;
    }
    NES_DEBUG("CoordinatorEngine::AddParent: source node " << childId << " exists");

    std::vector<NESTopologyEntryPtr>
        sensorParent = topologyManager->getNESTopologyPlan()->getNodeById(parentId);
    if (sensorParent.size() == 0) {
        NES_ERROR("CoordinatorEngine::AddParent: sensorParent node " << parentId << " does not exists");
        return false;
    } else if (sensorParent.size() > 1) {
        NES_ERROR("CoordinatorEngine::AddParent: more than one sensorParent node with id exists "
                  << parentId << " count=" << sensorParent.size());
        return false;
    }
    NES_DEBUG("CoordinatorEngine::AddParent: sensorParent node " << parentId << " exists");

    bool connected =
        topologyManager->getNESTopologyPlan()->getNESTopologyGraph()->hasLink(sensorNodes[0],
                                                                              sensorParent[0]);
    if (connected) {
        NES_ERROR("CoordinatorEngine::AddParent: nodes " << childId << " and " << parentId
                                                         << " already exists");
        return false;
    }

    NESTopologyLinkPtr
        link = topologyManager->getNESTopologyPlan()->createNESTopologyLink(sensorNodes[0],
                                                                            sensorParent[0], 1, 1);
    if (link) {
        NES_DEBUG("CoordinatorEngine::AddParent: created link successfully");
        return true;
    } else {
        NES_ERROR("CoordinatorEngine::AddParent: created NOT successfully added");
        return false;
    }
}

bool CoordinatorEngine::removeParent(size_t childId, size_t parentId) {
    NES_DEBUG("CoordinatorEngine::removeParent: childId=" << childId << " parentId=" << parentId);

    std::vector<NESTopologyEntryPtr>
        sensorNodes = topologyManager->getNESTopologyPlan()->getNodeById(childId);
    if (sensorNodes.size() == 0) {
        NES_ERROR("CoordinatorEngine::removeParent: source node " << childId << " does not exists");
        return false;
    } else if (sensorNodes.size() > 1) {
        NES_ERROR(
            "CoordinatorEngine::removeParent: more than one sensorNodes node with id exists " << childId
                                                                                              << " count="
                                                                                              << sensorNodes.size());
        return false;
    }
    NES_DEBUG("CoordinatorEngine::removeParent: source node " << childId << " exists");

    std::vector<NESTopologyEntryPtr>
        sensorParent = topologyManager->getNESTopologyPlan()->getNodeById(parentId);
    if (sensorParent.size() == 0) {
        NES_ERROR("CoordinatorEngine::removeParent: sensorParent node " << childId << " does not exists");
        return false;
    } else if (sensorParent.size() > 1) {
        NES_ERROR(
            "CoordinatorEngine::removeParent: more than one sensorParent node with id exists " << childId
                                                                                               << " count="
                                                                                               << sensorNodes.size());
        return false;
    }
    NES_DEBUG("CoordinatorEngine::AddParent: sensorParent node " << parentId << " exists");

    bool connected =
        topologyManager->getNESTopologyPlan()->getNESTopologyGraph()->hasLink(sensorNodes[0],
                                                                              sensorParent[0]);
    if (!connected) {
        NES_ERROR("CoordinatorEngine::removeParent: nodes " << childId << " and " << parentId
                                                            << " are not connected");
        return false;
    }
    NES_DEBUG("CoordinatorEngine::removeParent: nodes connected");

    NESTopologyLinkPtr
        link = topologyManager->getNESTopologyPlan()->getNESTopologyGraph()->getLink(sensorNodes[0],
                                                                                     sensorParent[0]);

    bool success =
        topologyManager->getNESTopologyPlan()->getNESTopologyGraph()->removeEdge(link->getId());
    if (!success) {
        NES_ERROR("CoordinatorEngine::removeParent: edge between  " << childId << " and " << parentId
                                                                    << " could not be removed");
        return false;
    }
    NES_DEBUG("CoordinatorEngine::removeParent: successful");
    return true;
}
}// namespace NES