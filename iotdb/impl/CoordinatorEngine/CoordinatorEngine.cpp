#include <CoordinatorEngine/CoordinatorEngine.hpp>
#include <Util/Logger.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <API/Schema.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {
size_t getIdFromIp(std::string ip) {
    std::hash<std::string> hashFn;
    boost::uuids::basic_random_generator<boost::mt19937> gen;
    boost::uuids::uuid u = gen();
    string uidString = boost::uuids::to_string(u);

    return hashFn(uidString + ip);
}

size_t CoordinatorEngine::registerNode(std::string address,
                                       size_t numberOfCPUs,
                                       std::string nodeProperties,
                                       NESNodeType type) {
    NESTopologyManager::getInstance().printNESTopologyPlan();

    NES_DEBUG("CoordinatorEngine: Register Node address=" << address
                                                        << " numberOfCpus=" << numberOfCPUs
                                                        << " nodeProperties=" << nodeProperties
                                                        << " type=" << type);
    //get unique id for the new node
    size_t id = getIdFromIp(address);

    NESTopologyEntryPtr nodePtr;
    if (type == NESNodeType::Sensor) {
        NES_DEBUG("CoordinatorEngine::registerNode: register sensor node");
        nodePtr = NESTopologyManager::getInstance().createNESSensorNode(id,
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
        if (!StreamCatalog::instance().testIfLogicalStreamExistsInSchemaMapping(
            streamConf.logicalStreamName)) {
            NES_ERROR(
                "CoordinatorEngine::registerNode: error logical stream" << streamConf.logicalStreamName
                                                                        << " does not exist when adding physical stream "
                                                                        << streamConf.physicalStreamName);
            throw Exception(
                "CoordinatorEngine::registerNode logical stream does not exist " + streamConf.logicalStreamName);
        }

        SchemaPtr schema = StreamCatalog::instance().getSchemaForLogicalStream(
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

        bool success = StreamCatalog::instance().addPhysicalStream(
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
        nodePtr = NESTopologyManager::getInstance().createNESWorkerNode(id, address, CPUCapacity::Value(numberOfCPUs));

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

    const NESTopologyEntryPtr kRootNode = NESTopologyManager::getInstance()
        .getRootNode();

    if (kRootNode == nodePtr) {
        NES_DEBUG("CoordinatorEngine::registerNode: tree is empty so this becomes new root");
    } else {
        NES_DEBUG("CoordinatorEngine::registerNode: add link to root node " << kRootNode << " of type"
                                                                            << kRootNode->getEntryType());
        NESTopologyManager::getInstance().createNESTopologyLink(nodePtr, kRootNode, 1, 1);
    }

    NES_DEBUG("CoordinatorEngine::registerNode: topology after insert = "
                  << NESTopologyManager::getInstance().getNESTopologyPlan()->getTopologyPlanString());

    return id;

}

bool CoordinatorEngine::unregisterNode(size_t queryId) {
    NES_DEBUG("CoordinatorEngine::UnregisterNode: try to disconnect sensor with id " << queryId);

    std::vector<NESTopologyEntryPtr> sensorNodes =
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(
            queryId);

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
            "CoordinatorEngine::UnregisterNode: more than one worker node found with id " << queryId
                                                                                          << " numberOfOccurrences="
                                                                                          << numberOfOccurrences);
        throw Exception("node is ambitious");
    } else if (numberOfOccurrences == 0) {
        NES_ERROR("CoordinatorActor: node with id not found " << queryId);
        throw Exception("node not found");
    }
    NES_DEBUG("CoordinatorEngine::UnregisterNode: found sensor, try to delete it in catalog");
    //remove from catalog
    bool successCatalog = StreamCatalog::instance().removePhysicalStreamByHashId(
        queryId);
    NES_DEBUG("CoordinatorEngine::UnregisterNode: success in catalog is " << successCatalog);

    NES_DEBUG("CoordinatorEngine::UnregisterNode: found sensor, try to delete it in toplogy");
    //remove from topology
    bool successTopology = NESTopologyManager::getInstance().removeNESNode(sensorNode);
    NES_DEBUG("CoordinatorEngine::UnregisterNode: success in topology is " << successTopology);

    return successCatalog && successTopology;

}

bool CoordinatorEngine::registerPhysicalStream(size_t queryId,
                                               std::string sourcetype,
                                               std::string sourceconf,
                                               size_t sourcefrequency,
                                               size_t numberofbufferstoproduce,
                                               std::string physicalstreamname,
                                               std::string logicalstreamname) {
    NES_DEBUG("CoordinatorEngine::RegisterPhysicalStream: try to register physical stream id " << queryId);

    PhysicalStreamConfig streamConf(sourcetype,
                                    sourceconf,
                                    sourcefrequency,
                                    numberofbufferstoproduce,
                                    physicalstreamname,
                                    logicalstreamname);

    NES_DEBUG(
        "CoordinatorEngine::RegisterPhysicalStream: try to register physical stream with conf= "
            << streamConf.toString() << " for workerId="
            << queryId);

    std::vector<NESTopologyEntryPtr> sensorNodes =
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(
            queryId);

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

    bool success = StreamCatalog::instance().addPhysicalStream(
        streamConf.logicalStreamName, sce);

    return success;
}

bool CoordinatorEngine::unregisterPhysicalStream(std::string physicalStreamName,
                                                 std::string logicalStreamName,
                                                 size_t queryId) {
    NES_DEBUG(
        "CoordinatorEngine::UnregisterPhysicalStream: try to remove physical "
        "stream with name "
            << physicalStreamName << " logical name "
            << logicalStreamName << " workerId=" << queryId);

    std::vector<NESTopologyEntryPtr> sensorNodes =
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(
            queryId);

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
            "CoordinatorEngine::UnregisterPhysicalStream: sensor not found with workerId" << queryId);
        return false;
    }
    NES_DEBUG("node type=" << sensorNode->getEntryTypeString());

    bool success = StreamCatalog::instance().removePhysicalStream(logicalStreamName,
                                                                  physicalStreamName,
                                                                  queryId);
    return success;
}

bool CoordinatorEngine::registerLogicalStream(std::string logicalStreamName, std::string schemaString) {
    NES_DEBUG("CoordinatorEngine::registerLogicalStream: register logical stream=" << logicalStreamName << " schema="
                                                                                   << schemaString);
    SchemaPtr schema = UtilityFunctions::createSchemaFromCode(schemaString);
    NES_DEBUG("StreamCatalogService: schema successfully created");
    bool success = StreamCatalog::instance().addLogicalStream(logicalStreamName, schema);
    return success;
}

bool CoordinatorEngine::unregisterLogicalStream(std::string logicalStreamName) {
    NES_DEBUG("CoordinatorEngine::unregisterLogicalStream: register logical stream=" << logicalStreamName);
    bool success = StreamCatalog::instance().removeLogicalStream(logicalStreamName);
    return success;
}

bool CoordinatorEngine::addParent(size_t childId, size_t parentId) {
    NES_DEBUG("CoordinatorEngine::addParent: childId=" << childId << " parentId=" << parentId);

    if (childId == parentId) {
        NES_ERROR("CoordinatorEngine::AddParent: cannot add link to itself");
        return false;
    }

    std::vector<NESTopologyEntryPtr>
        sensorNodes = NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(childId);
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
        sensorParent = NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(parentId);
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
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNESTopologyGraph()->hasLink(sensorNodes[0],
                                                                                               sensorParent[0]);
    if (connected) {
        NES_ERROR("CoordinatorEngine::AddParent: nodes " << childId << " and " << parentId
                                                         << " already exists");
        return false;
    }

    NESTopologyLinkPtr
        link = NESTopologyManager::getInstance().getNESTopologyPlan()->createNESTopologyLink(sensorNodes[0],
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
        sensorNodes = NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(childId);
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
        sensorParent = NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(parentId);
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
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNESTopologyGraph()->hasLink(sensorNodes[0],
                                                                                               sensorParent[0]);
    if (!connected) {
        NES_ERROR("CoordinatorEngine::removeParent: nodes " << childId << " and " << parentId
                                                                      << " are not connected");
        return false;
    }
    NES_DEBUG("CoordinatorEngine::removeParent: nodes connected");


    NESTopologyLinkPtr
        link = NESTopologyManager::getInstance().getNESTopologyPlan()->getNESTopologyGraph()->getLink(sensorNodes[0],
                                                                                                      sensorParent[0]);

    bool success =
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNESTopologyGraph()->removeEdge(link->getId());
    if (!success) {
        NES_ERROR("CoordinatorEngine::removeParent: edge between  " << childId << " and " << parentId
                                                                              << " could not be removed");
        return false;
    }
    NES_DEBUG("CoordinatorEngine::removeParent: successfull");
    return true;
}
}