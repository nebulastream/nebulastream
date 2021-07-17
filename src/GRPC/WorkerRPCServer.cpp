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

#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <GRPC/WorkerRPCServer.hpp>
#include <Monitoring/Metrics/MetricCatalog.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Monitoring/MonitoringAgent.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {

WorkerRPCServer::WorkerRPCServer(NodeEngine::NodeEnginePtr nodeEngine, MonitoringAgentPtr monitoringAgent)
    : nodeEngine(nodeEngine), monitoringAgent(monitoringAgent) {
    NES_DEBUG("WorkerRPCServer::WorkerRPCServer()");
}

Status WorkerRPCServer::RegisterQuery(ServerContext*, const RegisterQueryRequest* request, RegisterQueryReply* reply) {
    auto queryPlan = QueryPlanSerializationUtil::deserializeQueryPlan((SerializableQueryPlan*) &request->queryplan());
    NES_DEBUG("WorkerRPCServer::RegisterQuery: got request for queryId: " << queryPlan->getQueryId()
                                                                          << " plan=" << queryPlan->toString());
    bool success;
    try {
        success = nodeEngine->registerQueryInNodeEngine(queryPlan);
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

Status WorkerRPCServer::UnregisterQuery(ServerContext*, const UnregisterQueryRequest* request, UnregisterQueryReply* reply) {
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

Status WorkerRPCServer::StartQuery(ServerContext*, const StartQueryRequest* request, StartQueryReply* reply) {
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

Status WorkerRPCServer::StopQuery(ServerContext*, const StopQueryRequest* request, StopQueryReply* reply) {
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

Status
WorkerRPCServer::RegisterMonitoring(ServerContext*, const MonitoringRegistrationRequest* request, MonitoringRegistrationReply*) {
    try {
        NES_DEBUG("WorkerRPCServer::RegisterMonitoring: Got request");
        MonitoringPlanPtr plan = MonitoringPlan::create(request->monitoringplan());
        auto schema = monitoringAgent->registerMonitoringPlan(plan);
        if (schema) {
            return Status::OK;
        } else {
            return Status::CANCELLED;
        }
    } catch (std::exception& ex) {
        NES_ERROR("WorkerRPCServer: Registering monitoring plan failed: " << ex.what());
        return Status::CANCELLED;
    }
}

Status WorkerRPCServer::GetMonitoringData(ServerContext*, const MonitoringDataRequest*, MonitoringDataReply* reply) {
    try {
        NES_DEBUG("WorkerRPCServer::GetMonitoringData: Got request");
        auto buf = nodeEngine->getBufferManager()->getBufferBlocking();
        monitoringAgent->getMetrics(buf);

        // add buffer to the reply object
        reply->set_buffer(buf.getBuffer(), monitoringAgent->getSchema()->getSchemaSizeInBytes());
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("WorkerRPCServer: Requesting monitoring data failed: " << ex.what());
        return Status::CANCELLED;
    }
}

Status WorkerRPCServer::SetSourceConfig(ServerContext*, const SetSourceConfigRequest*, SetSourceConfigReply*) {
    try {
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("WorkerRPCServer: Setting source config failed: " << ex.what());
        return Status::CANCELLED;
    }
}

Status WorkerRPCServer::GetSourceConfig(ServerContext*, const GetSourceConfigRequest* request, GetSourceConfigReply* reply) {
    NES_DEBUG("WorkerRPCServer::GetSourceConfig: got request for " << request->physicalstreamname());
    try {
        auto conf = nodeEngine->getConfig(request->physicalstreamname());
        reply->set_sourceconfig(conf->toSourceConfig()->toJson());
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("WorkerRPCServer::GetSourceConfig: failed");
        return Status::CANCELLED;
    }
}
}// namespace NES