#include <GRPC/Serialization/OperatorSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <GRPC/WorkerRPCServer.hpp>
#include <Monitoring/Metrics/MetricCatalog.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Util/UtilityFunctions.hpp>

using namespace NES;

WorkerRPCServer::WorkerRPCServer(NodeEnginePtr nodeEngine)
    : nodeEngine(nodeEngine) {
    NES_DEBUG("WorkerRPCServer::WorkerRPCServer()");
}

Status WorkerRPCServer::RegisterQuery(ServerContext*, const RegisterQueryRequest* request,
                                      RegisterQueryReply* reply) {
    auto queryId = request->queryid();
    auto querySubPlanId = request->querysubplanid();

    auto queryPlan = OperatorSerializationUtil::deserializeOperator((SerializableOperator*) &request->operatortree());
    NES_DEBUG("WorkerRPCServer::RegisterQuery: got request for queryId: " << queryId);
    bool success;
    try {
        success = nodeEngine->registerQueryInNodeEngine(queryId, querySubPlanId, queryPlan);
    } catch (std::exception& error) {
        NES_ERROR("Register query crashed: " << error.what());
        success = false;
    }
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

Status WorkerRPCServer::UnregisterQuery(ServerContext*, const UnregisterQueryRequest* request,
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

Status WorkerRPCServer::StartQuery(ServerContext*, const StartQueryRequest* request,
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

Status WorkerRPCServer::StopQuery(ServerContext*, const StopQueryRequest* request,
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

Status WorkerRPCServer::RequestMonitoringData(ServerContext*, const MonitoringRequest* request, MonitoringReply* reply) {
    auto plan = MonitoringPlan{request->monitoringplan()};
    NES_DEBUG("WorkerRPCServer::RequestMonitoringData: Got request" << plan);

    MetricGroupPtr metricGroup = plan.createMetricGroup(MetricCatalog::NesMetrics());
    auto buf = nodeEngine->getBufferManager()->getBufferBlocking();

    auto schema = Schema::create();
    metricGroup->getSample(schema, buf);

    // add schema and buffer to the reply object
    SchemaSerializationUtil::serializeSchema(schema, reply->mutable_schema());
    reply->set_buffer(buf.getBuffer(), schema->getSchemaSizeInBytes());

    return Status::OK;
}
