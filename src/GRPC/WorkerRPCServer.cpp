#include <GRPC/Serialization/OperatorSerializationUtil.hpp>
#include <GRPC/WorkerRPCServer.hpp>

using namespace NES;

WorkerRPCServer::WorkerRPCServer(NodeEnginePtr nodeEngine)
    : nodeEngine(nodeEngine)
{
    NES_DEBUG("WorkerRPCServer::WorkerRPCServer");
}

Status WorkerRPCServer::RegisterQuery(ServerContext* context, const RegisterQueryRequest* request,
                                      RegisterQueryReply* reply) {
    auto queryId = request->queryid();

    auto queryPlan = OperatorSerializationUtil::deserializeOperator((SerializableOperator*) &request->operatortree());
    NES_DEBUG("WorkerRPCServer::RegisterQuery: got request for queryId: " << queryId);
    bool success = nodeEngine->registerQueryInNodeEngine(queryId, queryPlan);
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