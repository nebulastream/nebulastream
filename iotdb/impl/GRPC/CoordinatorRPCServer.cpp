#include <GRPC/CoordinatorRPCServer.hpp>
#include <Util/Logger.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

using namespace NES;

CoordinatorRPCServer::CoordinatorRPCServer() {
    coordinatorServicePtr = NES::CoordinatorService::getInstance();
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

    size_t id = getIdFromIp(request->address());
    PhysicalStreamConfig streamConf;
    NESTopologyEntryPtr node = coordinatorServicePtr->registerNode(
        id, request->address(), request->numberofcpus(),
        request->nodeproperties(), streamConf, (NESNodeType) request->type());

    if (!node) {
        NES_ERROR("CoordinatorRPCServer::RegisterNode : node not created");
        reply->set_id(0);//TODO: check on the other side
        return Status::CANCELLED;
    }
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
                                                                                             << " numberOfOccurrences=" << numberOfOccurrences);
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
    bool successTopology = coordinatorServicePtr->deregisterSensor(sensorNode);
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

    bool success = coordinatorServicePtr->addNewParentToSensorNode(request->childid(),
                                                                   request->parentid());

    if (success) {
        NES_DEBUG("CoordinatorRPCServer::AddParent success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("CoordinatorRPCServer::AddParent failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::RemoveParent(ServerContext* context, const RemoveParentRequest* request,
                                          RemoveParentReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RemoveParent: request =" << request);

    bool success = coordinatorServicePtr->removeParentFromSensorNode(request->childid(),
                                                                     request->parentid());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::RemoveParent success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("CoordinatorRPCServer::RemoveParent failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}
