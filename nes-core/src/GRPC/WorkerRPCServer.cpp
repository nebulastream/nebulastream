/*
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
#include <Plans/Query/QueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <utility>

namespace NES {

WorkerRPCServer::WorkerRPCServer(Runtime::NodeEnginePtr nodeEngine, MonitoringAgentPtr monitoringAgent)
    : nodeEngine(std::move(nodeEngine)), monitoringAgent(std::move(monitoringAgent)) {
    NES_DEBUG("WorkerRPCServer::WorkerRPCServer()");
}

Status WorkerRPCServer::RegisterQuery(ServerContext*, const RegisterQueryRequest* request, RegisterQueryReply* reply) {
    auto queryPlan = QueryPlanSerializationUtil::deserializeQueryPlan((SerializableQueryPlan*) &request->queryplan());
    NES_DEBUG("WorkerRPCServer::RegisterQuery: got request for queryId: " << queryPlan->getQueryId()
                                                                          << " plan=" << queryPlan->toString());
    bool success = 0;
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
    }
    NES_ERROR("WorkerRPCServer::RegisterQuery: failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status WorkerRPCServer::UnregisterQuery(ServerContext*, const UnregisterQueryRequest* request, UnregisterQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::UnregisterQuery: got request for " << request->queryid());
    bool success = nodeEngine->unregisterQuery(request->queryid());
    if (success) {
        NES_DEBUG("WorkerRPCServer::UnregisterQuery: success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("WorkerRPCServer::UnregisterQuery: failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status WorkerRPCServer::StartQuery(ServerContext*, const StartQueryRequest* request, StartQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::StartQuery: got request for " << request->queryid());
    bool success = nodeEngine->startQuery(request->queryid());
    if (success) {
        NES_DEBUG("WorkerRPCServer::StartQuery: success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("WorkerRPCServer::StartQuery: failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status WorkerRPCServer::StopQuery(ServerContext*, const StopQueryRequest* request, StopQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::StopQuery: got request for " << request->queryid());
    bool success = nodeEngine->stopQuery(request->queryid());
    if (success) {
        NES_DEBUG("WorkerRPCServer::StopQuery: success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("WorkerRPCServer::StopQuery: failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status
WorkerRPCServer::RegisterMonitoring(ServerContext*, const MonitoringRegistrationRequest* request, MonitoringRegistrationReply*) {
    try {
        NES_DEBUG("WorkerRPCServer::RegisterMonitoring: Got request");
        MonitoringPlanPtr plan = MonitoringPlan::create(request->monitoringplan());
        auto schema = monitoringAgent->registerMonitoringPlan(plan);
        if (schema) {
            return Status::OK;
        }
        return Status::CANCELLED;

    } catch (std::exception& ex) {
        NES_ERROR("WorkerRPCServer: Registering monitoring plan failed: " << ex.what());
        return Status::CANCELLED;
    }
}

Status WorkerRPCServer::GetMonitoringData(ServerContext*, const MonitoringDataRequest*, MonitoringDataReply* reply) {
    try {
        NES_DEBUG("WorkerRPCServer::GetMonitoringData: Got request");
        auto buf = nodeEngine->getBufferManager()->getUnpooledBuffer(monitoringAgent->getSchema()->getSchemaSizeInBytes());
        if (buf.has_value()) {
            monitoringAgent->getMetricsFromPlan(buf.value());
            // add buffer to the reply object
            reply->set_buffer(buf.value().getBuffer(), monitoringAgent->getSchema()->getSchemaSizeInBytes());
            return Status::OK;
        }
        NES_THROW_RUNTIME_ERROR("WorkerRPCServer: Error getting unpooled buffer.");
    } catch (std::exception& ex) {
        NES_ERROR("WorkerRPCServer: Requesting monitoring data failed: " << ex.what());
    }
    return Status::CANCELLED;
}
Status WorkerRPCServer::BeginBuffer(ServerContext*, const BufferRequest* request, BufferReply* reply) {
    NES_DEBUG("WorkerRPCServer::BeginBuffer request received");

    uint64_t querySubPlanId = request->querysubplanid();
    uint64_t uniqueNetworkSinkDescriptorId = request->uniquenetworksinkdescriptorid();
    bool success = nodeEngine->bufferData(querySubPlanId,uniqueNetworkSinkDescriptorId);
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
Status WorkerRPCServer::UpdateNetworkSink(ServerContext*, const UpdateNetworkSinkRequest* request,
                                           UpdateNetworkSinkReply* reply) {
    NES_DEBUG("WorkerRPCServer::Sink Reconfiguration request received");
    uint64_t querySubPlanId = request->querysubplanid();
    uint64_t uniqueNetworkSinkDescriptorId = request->uniquenetworksinkdescriptorid();
    uint64_t newNodeId = request->newnodeid();
    std::string newHostname = request->newhostname();
    uint32_t newPort = request->newport();

    bool success = nodeEngine->updateNetworkSink(newNodeId,newHostname,newPort,querySubPlanId,uniqueNetworkSinkDescriptorId);
    if (success) {
        NES_DEBUG("WorkerRPCServer::UpdateNetworkSinks: success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("WorkerRPCServer::UpdateNetworkSinks: failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}
}// namespace NES