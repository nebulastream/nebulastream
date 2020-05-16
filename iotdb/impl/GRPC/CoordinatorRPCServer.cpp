#include <GRPC/CoordinatorRPCServer.hpp>
#include <Util/Logger.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <Topology/NESTopologyManager.hpp>

using namespace NES;

CoordinatorRPCServer::CoordinatorRPCServer() {
};

size_t getIdFromIp(std::string ip) {
    std::hash<std::string> hashFn;
    boost::uuids::basic_random_generator<boost::mt19937> gen;
    boost::uuids::uuid u = gen();
    string uidString = boost::uuids::to_string(u);

    return hashFn(uidString + ip);
}

Status CoordinatorRPCServer::RegisterNode(ServerContext* context, const RegisterNodeRequest* request,
                                          RegisterNodeReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterNode: request =" << request);
    NESTopologyManager::getInstance().printNESTopologyPlan();

    NES_DEBUG("Coordinator RPC: Register Node address=" << request->address()
                                                        << " numberOfCpus=" << request->numberofcpus()
                                                        << " nodeProperties=" << request->nodeproperties()
                                                        << " type=" << request->type());
    //get unique id for the new node
    size_t id = getIdFromIp(request->address());

    NESTopologyEntryPtr nodePtr;
    if (request->type() == NESNodeType::Sensor) {
        NES_DEBUG("CoordinatorRPCServer::registerNode: register sensor node");
        nodePtr = NESTopologyManager::getInstance().createNESSensorNode(id,
                                                                        request->address(),
                                                                        CPUCapacity::Value(request->numberofcpus()));

        if (!nodePtr) {
            NES_ERROR("CoordinatorRPCServer::RegisterNode : node not created");
            reply->set_id(0);//TODO: check on the other side
            return Status::CANCELLED;
        }

        PhysicalStreamConfig streamConf;
        NESTopologySensorNode* sensor = dynamic_cast<NESTopologySensorNode*>(nodePtr.get());
        sensor->setPhysicalStreamName(streamConf.physicalStreamName);

        NES_DEBUG(
            "CoordinatorRPCServer::registerNode: try to register sensor phyName=" << streamConf.physicalStreamName
                                                                                  << " logName="
                                                                                  << streamConf.logicalStreamName
                                                                                  << " nodeID=" << nodePtr->getId());

        //check if logical stream exists
        if (!StreamCatalog::instance().testIfLogicalStreamExistsInSchemaMapping(
            streamConf.logicalStreamName)) {
            NES_ERROR(
                "CoordinatorRPCServer::registerNode: error logical stream" << streamConf.logicalStreamName
                                                                           << " does not exist when adding physical stream "
                                                                           << streamConf.physicalStreamName);
            throw Exception(
                "CoordinatorRPCServer::registerNode logical stream does not exist " + streamConf.logicalStreamName);
        }

        SchemaPtr schema = StreamCatalog::instance().getSchemaForLogicalStream(
            streamConf.logicalStreamName);

        DataSourcePtr source;
        if (streamConf.sourceType != "CSVSource"
            && streamConf.sourceType != "DefaultSource") {
            NES_ERROR(
                "CoordinatorRPCServer::registerNode: error source type " << streamConf.sourceType
                                                                         << " is not supported");
            throw Exception(
                "CoordinatorRPCServer::registerNode: error source type " + streamConf.sourceType
                    + " is not supported");
        }

        StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
            streamConf, nodePtr);

        bool success = StreamCatalog::instance().addPhysicalStream(
            streamConf.logicalStreamName, sce);
        if (!success) {
            NES_ERROR(
                "CoordinatorRPCServer::registerNode: physical stream " << streamConf.physicalStreamName
                                                                       << " could not be added to catalog");
            throw Exception(
                "CoordinatorRPCServer::registerNode: physical stream " + streamConf.physicalStreamName
                    + " could not be added to catalog");
        }

    } else if (request->type() == NESNodeType::Worker) {
        NES_DEBUG("CoordinatorRPCServer::registerNode: register worker node");
        nodePtr = NESTopologyManager::getInstance().createNESWorkerNode(id, request->address(), CPUCapacity::Value(request->numberofcpus()));

        if (!nodePtr) {
            NES_ERROR("CoordinatorRPCServer::RegisterNode : node not created");
            reply->set_id(0);//TODO: check on the other side
            return Status::CANCELLED;
        }
    } else {
        NES_ERROR("CoordinatorRPCServer::registerNode type not supported " << request->type());
        assert(0);
    }
    assert(nodePtr);

    if (request->nodeproperties() != "defaultProperties") {
        nodePtr->setNodeProperty(request->nodeproperties());
    }

    const NESTopologyEntryPtr kRootNode = NESTopologyManager::getInstance()
        .getRootNode();

    if (kRootNode == nodePtr) {
        NES_DEBUG("CoordinatorRPCServer::registerNode: tree is empty so this becomes new root");
    } else {
        NES_DEBUG("CoordinatorRPCServer::registerNode: add link to root node " << kRootNode << " of type"
                                                                               << kRootNode->getEntryType());
        NESTopologyManager::getInstance().createNESTopologyLink(nodePtr, kRootNode, 1, 1);
    }

    NES_DEBUG("CoordinatorRPCServer::registerNode: topology after insert");
    NESTopologyManager::getInstance().printNESTopologyPlan();
    reply->set_id(id);
    //TODO: do the bookkeeping
    return Status::OK;
}

Status CoordinatorRPCServer::UnregisterNode(ServerContext* context, const UnregisterNodeRequest* request,
                                            UnregisterNodeReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterNode: request =" << request);

    NES_DEBUG("CoordinatorRPCServer::UnregisterNode: try to disconnect sensor with id " << request->id());

    std::vector<NESTopologyEntryPtr> sensorNodes =
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(
            request->id());

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
            "CoordinatorRPCServer::UnregisterNode: more than one worker node found with id " << request->id()
                                                                                             << " numberOfOccurrences="
                                                                                             << numberOfOccurrences);
        throw Exception("node is ambitious");
    } else if (numberOfOccurrences == 0) {
        NES_ERROR("CoordinatorActor: node with id not found " << request->id());
        throw Exception("node not found");
    }
    NES_DEBUG("CoordinatorRPCServer::UnregisterNode: found sensor, try to delete it in catalog");
    //remove from catalog
    bool successCatalog = StreamCatalog::instance().removePhysicalStreamByHashId(
        request->id());
    NES_DEBUG("CoordinatorRPCServer::UnregisterNode: success in catalog is " << successCatalog);

    NES_DEBUG("CoordinatorRPCServer::UnregisterNode: found sensor, try to delete it in toplogy");
    //remove from topology
    bool successTopology =  NESTopologyManager::getInstance().removeNESNode(sensorNode);
    NES_DEBUG("CoordinatorRPCServer::UnregisterNode: success in topology is " << successTopology);

    if (successCatalog && successTopology) {
        NES_DEBUG("CoordinatorRPCServer::UnregisterNode: sensor successfully removed");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("CoordinatorRPCServer::UnregisterNode: sensor was not removed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::RegisterPhysicalStream(ServerContext* context,
                                                    const RegisterPhysicalStreamRequest* request,
                                                    RegisterPhysicalStreamReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterPhysicalStream: request =" << request);

    NES_DEBUG("CoordinatorRPCServer::RegisterPhysicalStream: try to register pyhsical stream id " << request->id()
                                                                                                  << " request="
                                                                                                  << request);

    PhysicalStreamConfig streamConf(request->sourcetype(),
                                    request->sourceconf(),
                                    request->sourcefrequency(),
                                    request->numberofbufferstoproduce(),
                                    request->physicalstreamname(),
                                    request->logicalstreamname());

    NES_DEBUG(
        "CoordinatorRPCServer::RegisterPhysicalStream: try to register physical stream with conf= "
            << streamConf.toString() << " for workerId="
            << request->id());

    std::vector<NESTopologyEntryPtr> sensorNodes =
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(
            request->id());

    if (sensorNodes.size() == 0) {
        NES_ERROR("CoordinatorRPCServer::RegisterPhysicalStream node not found");
        reply->set_success(false);
        return Status::CANCELLED;
    }
    if (sensorNodes.size() > 1) {
        NES_ERROR("CoordinatorRPCServer::RegisterPhysicalStream more than one node found");
        reply->set_success(false);
        return Status::CANCELLED;
    }

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
        streamConf, sensorNodes[0]);

    bool success = StreamCatalog::instance().addPhysicalStream(
        streamConf.logicalStreamName, sce);

    if (success) {
        NES_DEBUG("CoordinatorRPCServer::RegisterPhysicalStream success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("CoordinatorRPCServer::RegisterPhysicalStream failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::UnregisterPhysicalStream(ServerContext* context,
                                                      const UnregisterPhysicalStreamRequest* request,
                                                      UnregisterPhysicalStreamReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterPhysicalStream: request =" << request);

    NES_DEBUG(
        "CoordinatorRPCServer::UnregisterPhysicalStream: try to remove physical "
        "stream with name "
            << request->physicalstreamname() << " logical name "
            << request->logicalstreamname() << " workerId=" << request->id());

    std::vector<NESTopologyEntryPtr> sensorNodes =
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(
            request->id());

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
            "CoordinatorRPCServer::UnregisterPhysicalStream: sensor not found with workerId" << request->id());
        reply->set_success(false);
        return Status::CANCELLED;
    }
    NES_DEBUG("node type=" << sensorNode->getEntryTypeString());

    bool success = StreamCatalog::instance().removePhysicalStream(request->logicalstreamname(),
                                                                  request->physicalstreamname(),
                                                                  request->id());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::UnregisterPhysicalStream success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("CoordinatorRPCServer::UnregisterPhysicalStream failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::RegisterLogicalStream(ServerContext* context,
                                                   const RegisterLogicalStreamRequest* request,
                                                   RegisterLogicalStreamReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterLogicalStream: request =" << request);

    SchemaPtr schema = UtilityFunctions::createSchemaFromCode(request->streamschema());
    NES_DEBUG("StreamCatalogService: schema successfully created");
    bool success = StreamCatalog::instance().addLogicalStream(request->streamname(), schema);

    if (success) {
        NES_DEBUG("CoordinatorRPCServer::RegisterLogicalStream success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("CoordinatorRPCServer::RegisterLogicalStream failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::UnregisterLogicalStream(ServerContext* context,
                                                     const UnregisterLogicalStreamRequest* request,
                                                     UnregisterLogicalStreamReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterLogicalStream: request =" << request);

    bool success = StreamCatalog::instance().removeLogicalStream(request->streamname());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::UnregisterLogicalStream success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("CoordinatorRPCServer::UnregisterLogicalStream failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::AddParent(ServerContext* context, const AddParentRequest* request,
                                       AddParentReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterLogicalStream: request =" << request);

    if (request->childid() == request->parentid()) {
        NES_ERROR("CoordinatorRPCServer::AddParent: cannot add link to itself");
        reply->set_success(false);
        return Status::CANCELLED;
    }

    std::vector<NESTopologyEntryPtr> sensorNodes = NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(request->childid());
    if (sensorNodes.size() == 0) {
        NES_ERROR("CoordinatorRPCServer::AddParent: source node " << request->childid() << " does not exists");
        reply->set_success(false);
        return Status::CANCELLED;
    } else if (sensorNodes.size() > 1) {
        NES_ERROR(
            "CoordinatorRPCServer::AddParent: more than one sensorNodes node with id exists " << request->childid()
                                                                                                         << " count="
                                                                                                         << sensorNodes.size());
        reply->set_success(false);
        return Status::CANCELLED;
    }
    NES_ERROR("CoordinatorRPCServer::AddParent: source node " << request->childid() << " exists");

    std::vector<NESTopologyEntryPtr> sensorParent = NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(request->parentid());
    if (sensorParent.size() == 0) {
        NES_ERROR("CoordinatorRPCServer::AddParent: sensorParent node " << request->parentid() << " does not exists");
        reply->set_success(false);
        return Status::CANCELLED;
    } else if (sensorParent.size() > 1) {
        NES_ERROR("CoordinatorRPCServer::AddParent: more than one sensorParent node with id exists "
                      << request->parentid() << " count=" << sensorParent.size());
        reply->set_success(false);
        return Status::CANCELLED;
    }
    NES_ERROR("CoordinatorRPCServer::AddParent: sensorParent node " << request->parentid() << " exists");

    bool connected = NESTopologyManager::getInstance().getNESTopologyPlan()->getNESTopologyGraph()->hasLink(sensorNodes[0],
                                                                                          sensorParent[0]);
    if (connected) {
        NES_ERROR("CoordinatorRPCServer::AddParent: nodes " << request->childid() << " and " << request->parentid()
                                                                       << " already exists");
        reply->set_success(false);
        return Status::CANCELLED;
    }

    NESTopologyLinkPtr link = NESTopologyManager::getInstance().getNESTopologyPlan()->createNESTopologyLink(sensorNodes[0],
                                                                                          sensorParent[0], 1, 1);
    if (link) {
        NES_DEBUG("CoordinatorRPCServer::AddParent: created link successfully");
    } else {
        NES_ERROR("CoordinatorRPCServer::AddParent: created NOT successfully added");
        reply->set_success(false);
        return Status::CANCELLED;
    }

    NES_DEBUG("CoordinatorRPCServer::AddParent success");
    reply->set_success(true);
    return Status::OK;
}

Status CoordinatorRPCServer::RemoveParent(ServerContext* context, const RemoveParentRequest* request,
                                          RemoveParentReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RemoveParent: request =" << request);

    std::vector<NESTopologyEntryPtr> sensorNodes = NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(request->childid());
    if (sensorNodes.size() == 0) {
        NES_ERROR("CoordinatorService::change_sensor_position: source node " << request->childid() << " does not exists");
        reply->set_success(false);
        return Status::CANCELLED;
    } else if (sensorNodes.size() > 1) {
        NES_ERROR(
            "CoordinatorService::change_sensor_position: more than one sensorNodes node with id exists " << request->childid()
                                                                                                         << " count="
                                                                                                         << sensorNodes.size());
        reply->set_success(false);
        return Status::CANCELLED;
    }

    std::vector<NESTopologyEntryPtr> sensorParent = NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(request->parentid());
    if (sensorParent.size() == 0) {
        NES_ERROR("CoordinatorService::change_sensor_position: sensorParent node " << request->childid() << " does not exists");
        reply->set_success(false);
        return Status::CANCELLED;
    } else if (sensorParent.size() > 1) {
        NES_ERROR(
            "CoordinatorService::change_sensor_position: more than one sensorParent node with id exists " << request->childid()
                                                                                                          << " count="
                                                                                                          << sensorNodes.size());
        reply->set_success(false);
        return Status::CANCELLED;
    }

    bool connected = NESTopologyManager::getInstance().getNESTopologyPlan()->getNESTopologyGraph()->hasLink(sensorNodes[0],
                                                                                          sensorParent[0]);
    if (!connected) {
        NES_ERROR("CoordinatorService::change_sensor_position: nodes " << request->childid() << " and " << request->parentid()
                                                                       << " are not connected");
        reply->set_success(false);
        return Status::CANCELLED;
    }

    NESTopologyLinkPtr link = NESTopologyManager::getInstance().getNESTopologyPlan()->getNESTopologyGraph()->getLink(sensorNodes[0],
                                                                                                   sensorParent[0]);

    bool success = NESTopologyManager::getInstance().getNESTopologyPlan()->getNESTopologyGraph()->removeEdge(link->getId());
    if (!success) {
        NES_ERROR("CoordinatorService::change_sensor_position: edge between  " << request->childid() << " and " << request->parentid()
                                                                               << " could not be removed");
        reply->set_success(false);
        return Status::CANCELLED;
    }

    NES_DEBUG("CoordinatorRPCServer::RemoveParent success");
    reply->set_success(true);
    return Status::OK;

}
