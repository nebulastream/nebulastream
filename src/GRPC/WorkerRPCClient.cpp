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

WorkerRPCClient::~WorkerRPCClient() { NES_DEBUG("~WorkerRPCClient()"); }

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

bool WorkerRPCClient::registerQueryAsync(std::string address, QueryPlanPtr queryPlan, CompletionQueuePtr cq) {
    QueryId queryId = queryPlan->getQueryId();
    QuerySubPlanId querySubPlanId = queryPlan->getQuerySubPlanId();
    NES_DEBUG("WorkerRPCClient::registerQueryAsync address=" << address << " queryId=" << queryId
                                                             << " querySubPlanId = " << querySubPlanId);

    // wrap the query id and the query operators in the protobuf register query request object.
    RegisterQueryRequest request;
    // serialize query plan.
    auto serializedQueryPlan = QueryPlanSerializationUtil::serializeQueryPlan(queryPlan);
    request.set_allocated_queryplan(serializedQueryPlan);

    NES_TRACE("WorkerRPCClient:registerQuery -> " << request.DebugString());
    RegisterQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(channel);

    // Call object to store rpc data
    AsyncClientCall<RegisterQueryReply>* call = new AsyncClientCall<RegisterQueryReply>;

    // workerStub->PrepareAsyncRegisterQuery() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->responseReader = workerStub->PrepareAsyncRegisterQuery(&call->context, request, cq.get());

    // StartCall initiates the RPC call
    call->responseReader->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call object.
    call->responseReader->Finish(&call->reply, &call->status, (void*) call);

    return true;
}

bool WorkerRPCClient::checkAsyncResult(std::map<CompletionQueuePtr, uint64_t> queues, RpcClientModes mode) {
    NES_DEBUG("start checkAsyncResult for mode=" << mode << " for " << queues.size() << " queues");
    bool result = true;
    for (auto& queue : queues) {
        //wait for all deploys to come back
        void* got_tag;
        bool ok = false;
        uint64_t cnt = 0;
        // Block until the next result is available in the completion queue "completionQueue".
        while (cnt != queue.second && queue.first->Next(&got_tag, &ok)) {
            // The tag in this example is the memory location of the call object
            bool status = false;
            if (mode == Register) {
                AsyncClientCall<RegisterQueryReply>* call = static_cast<AsyncClientCall<RegisterQueryReply>*>(got_tag);
                status = call->status.ok();
                delete call;
            } else if (mode == Unregister) {
                AsyncClientCall<UnregisterQueryReply>* call = static_cast<AsyncClientCall<UnregisterQueryReply>*>(got_tag);
                status = call->status.ok();
                delete call;
            } else if (mode == Start) {
                AsyncClientCall<StartQueryReply>* call = static_cast<AsyncClientCall<StartQueryReply>*>(got_tag);
                status = call->status.ok();
                delete call;
            } else if (mode == Stop) {
                AsyncClientCall<StopQueryReply>* call = static_cast<AsyncClientCall<StopQueryReply>*>(got_tag);
                status = call->status.ok();
                delete call;
            } else {
                NES_NOT_IMPLEMENTED();
            }

            if (!status) {
                NES_THROW_RUNTIME_ERROR("Deployment RPC failed, a scheduled async call for mode" << mode << " failed");
            }

            // Once we're complete, deallocate the call object.
            cnt++;
        }
    }
    NES_DEBUG("checkAsyncResult for mode=" << mode << " succeed");
    return result;
}
bool WorkerRPCClient::unregisterQueryAsync(std::string address, QueryId queryId, CompletionQueuePtr cq) {
    NES_DEBUG("WorkerRPCClient::unregisterQueryAsync address=" << address << " queryId=" << queryId);

    UnregisterQueryRequest request;
    request.set_queryid(queryId);

    UnregisterQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    // Call object to store rpc data
    AsyncClientCall<UnregisterQueryReply>* call = new AsyncClientCall<UnregisterQueryReply>;

    // workerStub->PrepareAsyncRegisterQuery() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->responseReader = workerStub->PrepareAsyncUnregisterQuery(&call->context, request, cq.get());

    // StartCall initiates the RPC call
    call->responseReader->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call object.
    call->responseReader->Finish(&call->reply, &call->status, (void*) call);

    return true;
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

bool WorkerRPCClient::startQueryAsyn(std::string address, QueryId queryId, CompletionQueuePtr cq) {
    NES_DEBUG("WorkerRPCClient::startQueryAsync address=" << address << " queryId=" << queryId);

    StartQueryRequest request;
    request.set_queryid(queryId);

    StartQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    // Call object to store rpc data
    AsyncClientCall<StartQueryReply>* call = new AsyncClientCall<StartQueryReply>;

    // workerStub->PrepareAsyncRegisterQuery() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->responseReader = workerStub->PrepareAsyncStartQuery(&call->context, request, cq.get());

    // StartCall initiates the RPC call
    call->responseReader->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call object.
    call->responseReader->Finish(&call->reply, &call->status, (void*) call);

    return true;
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
        NES_ERROR(" WorkerRPCClient::stopQuery "
                  "error="
                  << status.error_code() << ": " << status.error_message());
        throw Exception("Error while WorkerRPCClient::stopQuery");
    }
}

bool WorkerRPCClient::stopQueryAsync(std::string address, QueryId queryId, CompletionQueuePtr cq) {
    NES_DEBUG("WorkerRPCClient::stopQueryAsync address=" << address << " queryId=" << queryId);

    StopQueryRequest request;
    request.set_queryid(queryId);

    StopQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    // Call object to store rpc data
    AsyncClientCall<StopQueryReply>* call = new AsyncClientCall<StopQueryReply>;

    // workerStub->PrepareAsyncRegisterQuery() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->responseReader = workerStub->PrepareAsyncStopQuery(&call->context, request, cq.get());

    // StartCall initiates the RPC call
    call->responseReader->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call object.
    call->responseReader->Finish(&call->reply, &call->status, (void*) call);

    return true;
}

bool WorkerRPCClient::registerMonitoringPlan(const std::string& address, MonitoringPlanPtr plan) {
    NES_DEBUG("WorkerRPCClient: Monitoring request address=" << address);

    MonitoringRegistrationRequest request;
    request.mutable_monitoringplan()->CopyFrom(plan->serialize());

    ClientContext context;
    MonitoringRegistrationReply reply;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->RegisterMonitoring(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::RequestMonitoringData: status ok");
        return true;
    } else {
        NES_THROW_RUNTIME_ERROR(" WorkerRPCClient::RequestMonitoringData error=" + std::to_string(status.error_code()) + ": "
                                + status.error_message());
    }
    return false;
}

bool WorkerRPCClient::requestMonitoringData(const std::string& address, NodeEngine::TupleBuffer& buf, uint64_t schemaSizeBytes) {
    NES_DEBUG("WorkerRPCClient: Monitoring request address=" << address);
    MonitoringDataRequest request;
    ClientContext context;
    MonitoringDataReply reply;
    reply.set_buffer(buf.getBuffer<char>());

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->GetMonitoringData(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::RequestMonitoringData: status ok");
        memcpy(buf.getBuffer<char>(), reply.buffer().data(), schemaSizeBytes);
        buf.setNumberOfTuples(1);
        reply.release_buffer();
        return true;
    } else {

        // We need to release reply's buffer (in case we handle the exception).
        reply.release_buffer();
        NES_THROW_RUNTIME_ERROR(" WorkerRPCClient::RequestMonitoringData error=" << std::to_string(status.error_code()) << ": "
                                                                                 << status.error_message());
    }
    return false;
}
std::tuple<bool, std::string>
WorkerRPCClient::setSourceConfig(const std::string& address, const std::string& physicalStreamName, const std::string& sourceConfig) {
    NES_DEBUG("WorkerRPCClient: SourceConfig request address=" << address << " physicalStreamName=" << physicalStreamName);
    SetSourceConfigRequest request;
    ClientContext context;
    SetSourceConfigReply reply;

    request.set_physicalstreamname(physicalStreamName);
    request.set_sourceconfig(sourceConfig);

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->SetSourceConfig(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::GetSourceConfig: status ok");
        return {reply.success(), reply.sourceconfig()};
    } else {
        NES_THROW_RUNTIME_ERROR(" WorkerRPCClient::GetSourceConfig error=" << std::to_string(status.error_code()) << ": "
                                                                           << status.error_message());
    }
}
std::tuple<bool, std::string> WorkerRPCClient::getSourceConfig(const std::string& address,
                                                               const std::string& physicalStreamName) {
    NES_DEBUG("WorkerRPCClient: SourceConfig request address=" << address << " physicalStreamName=" << physicalStreamName);
    GetSourceConfigRequest request;
    ClientContext context;
    GetSourceConfigReply reply;

    request.set_physicalstreamname(physicalStreamName);

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->GetSourceConfig(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::GetSourceConfig: status ok");
        return {reply.success(), reply.sourceconfig()};
    } else {
        NES_THROW_RUNTIME_ERROR(" WorkerRPCClient::GetSourceConfig error=" << std::to_string(status.error_code()) << ": "
                                                                           << status.error_message());
    }
}

}// namespace NES
