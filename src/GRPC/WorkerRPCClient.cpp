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

#include <API/Schema.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <NodeEngine/TupleBuffer.hpp>

#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

WorkerRPCClient::WorkerRPCClient() { NES_DEBUG("WorkerRPCClient()"); }
bool WorkerRPCClient::deployQuery(std::string address, std::string executableTransferObject) {
    NES_DEBUG("WorkerRPCClient::deployQuery address=" << address << " eto=" << executableTransferObject);

    DeployQueryRequest request;

    DeployQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->DeployQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::deployQuery: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_DEBUG(" WorkerRPCClient::deployQuery "
                  "error="
                  << status.error_code() << ": " << status.error_message());
        throw Exception("Error while WorkerRPCClient::deployQuery");
    }
}

WorkerRPCClient::~WorkerRPCClient() { NES_DEBUG("~WorkerRPCClient()"); }

bool WorkerRPCClient::undeployQuery(std::string address, QueryId queryId) {
    NES_DEBUG("WorkerRPCClient::undeployQuery address=" << address << " queryId=" << queryId);

    UndeployQueryRequest request;
    request.set_queryid(queryId);

    UndeployQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->UndeployQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::undeployQuery: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_DEBUG(" WorkerRPCClient::undeployQuery "
                  "error="
                  << status.error_code() << ": " << status.error_message());
        throw Exception("Error while WorkerRPCClient::undeployQuery");
    }
}

bool WorkerRPCClient::registerQuery(std::string address, QueryPlanPtr queryPlan) {
    QueryId queryId = queryPlan->getQueryId();
    QuerySubPlanId querySubPlanId = queryPlan->getQuerySubPlanId();
    NES_DEBUG("WorkerRPCClient::registerQuery address=" << address << " queryId=" << queryId
                                                        << " querySubPlanId = " << querySubPlanId);

    // wrap the query id and the query operators in the protobuf register query request object.
    RegisterQueryRequest request;
    // serialize query plan.
    auto serializedQueryPlan = QueryPlanSerializationUtil::serializeQueryPlan(queryPlan);
    request.set_allocated_queryplan(serializedQueryPlan);

    NES_TRACE("WorkerRPCClient:registerQuery -> " << request.DebugString());
    RegisterQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->RegisterQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::registerQuery: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_DEBUG(" WorkerRPCClient::registerQuery "
                  "error="
                  << status.error_code() << ": " << status.error_message());
        throw Exception("Error while WorkerRPCClient::registerQuery");
    }
}

bool WorkerRPCClient::unregisterQuery(std::string address, QueryId queryId) {
    NES_DEBUG("WorkerRPCClient::unregisterQuery address=" << address << " queryId=" << queryId);

    UnregisterQueryRequest request;
    request.set_queryid(queryId);

    UnregisterQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->UnregisterQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::unregisterQuery: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_DEBUG(" WorkerRPCClient::unregisterQuery "
                  "error="
                  << status.error_code() << ": " << status.error_message());
        throw Exception("Error while WorkerRPCClient::unregisterQuery");
    }
}

bool WorkerRPCClient::startQuery(std::string address, QueryId queryId) {
    NES_DEBUG("WorkerRPCClient::startQuery address=" << address << " queryId=" << queryId);

    StartQueryRequest request;
    request.set_queryid(queryId);

    StartQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    Status status = workerStub->StartQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::startQuery: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_DEBUG(" WorkerRPCClient::startQuery "
                  "error="
                  << status.error_code() << ": " << status.error_message());
        throw Exception("Error while WorkerRPCClient::startQuery");
    }
}

bool WorkerRPCClient::stopQuery(std::string address, QueryId queryId) {

    NES_DEBUG("WorkerRPCClient::stopQuery address=" << address << " queryId=" << queryId);

    StopQueryRequest request;
    request.set_queryid(queryId);

    StopQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->StopQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::stopQuery: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_DEBUG(" WorkerRPCClient::stopQuery "
                  "error="
                  << status.error_code() << ": " << status.error_message());
        throw Exception("Error while WorkerRPCClient::stopQuery");
    }
}

SchemaPtr WorkerRPCClient::requestMonitoringData(const std::string& address, MonitoringPlanPtr plan, NodeEngine::TupleBuffer& buf) {
    NES_DEBUG("WorkerRPCClient: Monitoring request address=" << address);

    MonitoringRequest request;
    request.mutable_monitoringplan()->CopyFrom(plan->serialize());

    ClientContext context;
    MonitoringReply reply;
    reply.set_buffer(buf.getBufferAs<char>());

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->RequestMonitoringData(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::RequestMonitoringData: status ok");
        auto parsedSchema = SchemaSerializationUtil::deserializeSchema(reply.mutable_schema());
        memcpy(buf.getBufferAs<char>(), reply.buffer().data(), parsedSchema->getSchemaSizeInBytes());
        buf.setNumberOfTuples(1);
        reply.release_buffer();

        return parsedSchema;
    } else {
        NES_THROW_RUNTIME_ERROR(" WorkerRPCClient::RequestMonitoringData error=" + std::to_string(status.error_code()) + ": "
                                + status.error_message());
    }
}

}// namespace NES
