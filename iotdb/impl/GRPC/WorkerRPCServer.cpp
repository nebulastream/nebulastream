#include <GRPC/WorkerRPCServer.hpp>

using namespace NES;

WorkerRPCServer::WorkerRPCServer(NodeEnginePtr nodeEngine)
    : nodeEngine(nodeEngine) {
    NES_DEBUG("WorkerRPCServer::WorkerRPCServer");
}

Status WorkerRPCServer::DeployQuery(ServerContext* context, const DeployQueryRequest* request,
                                    DeployQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::DeployQuery: got request for " << request->eto());
    bool success = nodeEngine->deployQueryInNodeEngine(request->eto());
    if (success) {
        NES_DEBUG("WorkerRPCServer::DeployQuery: success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("WorkerRPCServer::DeployQuery: failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status WorkerRPCServer::UndeployQuery(ServerContext* context, const UndeployQueryRequest* request,
                                      UndeployQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::UndeployQuery: got request for " << request->queryid());
    bool success = nodeEngine->undeployQuery(request->queryid());
    if (success) {
        NES_DEBUG("WorkerRPCServer::UndeployQuery: success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("WorkerRPCServer::UndeployQuery: failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status WorkerRPCServer::RegisterQuery(ServerContext* context, const RegisterQueryRequest* request,
                                      RegisterQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::RegisterQuery: got request for " << request->eto());
    bool success = nodeEngine->registerQueryInNodeEngine(request->eto());
    if (success) {
        NES_DEBUG("WorkerRPCServer::RegisterQuery: success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("WorkerRPCServer::RegisterQuery: failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status WorkerRPCServer::UnregisterQuery(ServerContext* context, const UnregisterQueryRequest* request,
                                        UnregisterQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::UnregisterQuery: got request for " << request->queryid());
    bool success = nodeEngine->unregisterQuery(request->queryid());
    if (success) {
        NES_DEBUG("WorkerRPCServer::UnregisterQuery: success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("WorkerRPCServer::UnregisterQuery: failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status WorkerRPCServer::StartQuery(ServerContext* context, const StartQueryRequest* request,
                                   StartQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::StartQuery: got request for " << request->queryid());
    bool success = nodeEngine->startQuery(request->queryid());
    if (success) {
        NES_DEBUG("WorkerRPCServer::StartQuery: success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("WorkerRPCServer::StartQuery: failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status WorkerRPCServer::StopQuery(ServerContext* context, const StopQueryRequest* request,
                                  StopQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::StopQuery: got request for " << request->queryid());
    bool success = nodeEngine->stopQuery(request->queryid());
    if (success) {
        NES_DEBUG("WorkerRPCServer::StopQuery: success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("WorkerRPCServer::StopQuery: failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}