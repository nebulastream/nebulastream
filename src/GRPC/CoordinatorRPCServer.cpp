#include <CoordinatorEngine/CoordinatorEngine.hpp>
#include <GRPC/CoordinatorRPCServer.hpp>
#include <Util/Logger.hpp>
using namespace NES;

CoordinatorRPCServer::CoordinatorRPCServer(CoordinatorEnginePtr coordinatorEngine) : coordinatorEngine(coordinatorEngine){};

Status CoordinatorRPCServer::RegisterNode(ServerContext*, const RegisterNodeRequest* request,
                                          RegisterNodeReply* reply) {
    NES_DEBUG("CoordinatorEngine::RegisterNode: request =" << request);

    size_t id = coordinatorEngine->registerNode(request->address(),
                                                request->grpcport(),
                                                request->dataport(),
                                                request->numberofcpus(),
                                                request->nodeproperties(),
                                                (NodeType) request->type());
    if (id != 0) {
        NES_DEBUG("CoordinatorRPCServer::RegisterNode: success id=" << id);
        reply->set_id(id);
        return Status::OK;
    } else {
        NES_DEBUG("CoordinatorRPCServer::RegisterNode: failed");
        reply->set_id(0);
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::UnregisterNode(ServerContext*, const UnregisterNodeRequest* request,
                                            UnregisterNodeReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterNode: request =" << request);

    bool success = coordinatorEngine->unregisterNode(request->id());
    if (success) {
        NES_DEBUG("CoordinatorRPCServer::UnregisterNode: sensor successfully removed");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("CoordinatorRPCServer::UnregisterNode: sensor was not removed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status CoordinatorRPCServer::RegisterPhysicalStream(ServerContext*,
                                                    const RegisterPhysicalStreamRequest* request,
                                                    RegisterPhysicalStreamReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterPhysicalStream: request =" << request);

    bool success = coordinatorEngine->registerPhysicalStream(request->id(),
                                                             request->sourcetype(),
                                                             request->sourceconf(),
                                                             request->sourcefrequency(),
                                                             request->numberofbufferstoproduce(),
                                                             request->physicalstreamname(),
                                                             request->logicalstreamname());

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

Status CoordinatorRPCServer::UnregisterPhysicalStream(ServerContext*,
                                                      const UnregisterPhysicalStreamRequest* request,
                                                      UnregisterPhysicalStreamReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterPhysicalStream: request =" << request);

    bool success = coordinatorEngine->unregisterPhysicalStream(request->id(),
                                                               request->physicalstreamname(),
                                                               request->logicalstreamname());

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

Status CoordinatorRPCServer::RegisterLogicalStream(ServerContext*,
                                                   const RegisterLogicalStreamRequest* request,
                                                   RegisterLogicalStreamReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RegisterLogicalStream: request =" << request);

    bool success = coordinatorEngine->registerLogicalStream(request->streamname(), request->streamschema());

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

Status CoordinatorRPCServer::UnregisterLogicalStream(ServerContext*,
                                                     const UnregisterLogicalStreamRequest* request,
                                                     UnregisterLogicalStreamReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::UnregisterLogicalStream: request =" << request);

    bool success = coordinatorEngine->unregisterLogicalStream(request->streamname());
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

Status CoordinatorRPCServer::AddParent(ServerContext*, const AddParentRequest* request,
                                       AddParentReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::AddParent: request =" << request);

    bool success = coordinatorEngine->addParent(request->childid(), request->parentid());
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

Status CoordinatorRPCServer::RemoveParent(ServerContext*, const RemoveParentRequest* request,
                                          RemoveParentReply* reply) {
    NES_DEBUG("CoordinatorRPCServer::RemoveParent: request =" << request);

    bool success = coordinatorEngine->removeParent(request->childid(), request->parentid());
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
