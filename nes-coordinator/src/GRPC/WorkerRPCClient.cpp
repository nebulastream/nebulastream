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

#include <API/Schema.hpp>
#include <Exceptions/RpcException.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Health.grpc.pb.h>
#include <Monitoring/MonitoringPlan.hpp>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/GeoLocation.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES {

WorkerRPCClientPtr WorkerRPCClient::create() { return std::make_shared<WorkerRPCClient>(WorkerRPCClient()); }

bool WorkerRPCClient::registerQuery(const std::string& address, const DecomposedQueryPlanPtr& decomposedQueryPlan) {
    SharedQueryId sharedQueryId = decomposedQueryPlan->getSharedQueryId();
    auto decomposedQueryPlanId = decomposedQueryPlan->getDecomposedQueryPlanId();
    NES_DEBUG("WorkerRPCClient::registerQuery address={} sharedQueryId={} decomposedQueryPlanId = {} ",
              address,
              sharedQueryId,
              decomposedQueryPlanId);

    // wrap the query id and the query operators in the protobuf register query request object.
    RegisterQueryRequest request;

    // serialize query plan.
    auto serializedQueryPlan = request.mutable_decomposedqueryplan();
    DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(decomposedQueryPlan, serializedQueryPlan);

    NES_TRACE("WorkerRPCClient:registerQuery -> {}", request.DebugString());
    RegisterQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->RegisterQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::registerQuery: status ok return success={}", reply.success());
        return reply.success();
    }
    NES_DEBUG(" WorkerRPCClient::registerQuery "
              "error={}: {}",
              status.error_code(),
              status.error_message());
    throw Exceptions::RuntimeException("Error while WorkerRPCClient::registerQuery");
}

void WorkerRPCClient::registerQueryAsync(const std::string& address,
                                         const DecomposedQueryPlanPtr& decomposedQueryPlan,
                                         const CompletionQueuePtr& cq) {
    SharedQueryId sharedQueryId = decomposedQueryPlan->getSharedQueryId();
    DecomposedQueryPlanId decomposedQueryPlanId = decomposedQueryPlan->getDecomposedQueryPlanId();
    NES_DEBUG("WorkerRPCClient::registerQueryAsync address={} sharedQueryId={} decomposedQueryPlanId = {}",
              address,
              sharedQueryId,
              decomposedQueryPlanId);

    // wrap the query id and the query operators in the protobuf register query request object.
    RegisterQueryRequest request;
    // serialize query plan.
    auto serializableQueryPlan = request.mutable_decomposedqueryplan();
    DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(decomposedQueryPlan, serializableQueryPlan);

    NES_TRACE("WorkerRPCClient:registerQuery -> {}", request.DebugString());
    RegisterQueryReply reply;
    ClientContext context;

    grpc::ChannelArguments args;
    args.SetInt("test_key", decomposedQueryPlanId.getRawValue());
    std::shared_ptr<::grpc::Channel> channel = grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args);
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(channel);

    // Call object to store rpc data
    auto* call = new AsyncClientCall<RegisterQueryReply>;

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
}

void WorkerRPCClient::checkAsyncResult(const std::vector<RpcAsyncRequest>& rpcAsyncRequests) {
    NES_DEBUG("start checkAsyncResult for {} requests", rpcAsyncRequests.size());
    std::vector<RpcAsyncRequest> failedRPCCalls;
    for (const auto& rpcAsyncRequest : rpcAsyncRequests) {
        //wait for all deploys to come back
        void* got_tag = nullptr;
        bool ok = false;
        // Block until the next result is available in the completion queue "completionQueue".
        rpcAsyncRequest.completionQueue->Next(&got_tag, &ok);
        // The tag in this example is the memory location of the call object
        bool status;
        auto requestClientMode = rpcAsyncRequest.rpcClientMode;
        if (requestClientMode == RpcClientMode::Register) {
            auto* call = static_cast<AsyncClientCall<RegisterQueryReply>*>(got_tag);
            status = call->status.ok();
            delete call;
        } else if (requestClientMode == RpcClientMode::Unregister) {
            auto* call = static_cast<AsyncClientCall<UnregisterQueryReply>*>(got_tag);
            status = call->status.ok();
            delete call;
        } else if (requestClientMode == RpcClientMode::Start) {
            auto* call = static_cast<AsyncClientCall<StartQueryReply>*>(got_tag);
            status = call->status.ok();
            delete call;
        } else if (requestClientMode == RpcClientMode::Stop) {
            auto* call = static_cast<AsyncClientCall<StopQueryReply>*>(got_tag);
            status = call->status.ok();
            delete call;
        } else {
            NES_NOT_IMPLEMENTED();
        }

        if (!status) {
            failedRPCCalls.push_back(rpcAsyncRequest);
        }
    }
    if (!failedRPCCalls.empty()) {
        throw Exceptions::RpcException("Some RPCs did not succeed", failedRPCCalls);
    }
    NES_DEBUG("All rpc async requests succeeded");
}

void WorkerRPCClient::unregisterQueryAsync(const std::string& address,
                                           SharedQueryId sharedQueryId,
                                           const CompletionQueuePtr& cq) {
    NES_DEBUG("WorkerRPCClient::unregisterQueryAsync address={} queryId={}", address, sharedQueryId);

    UnregisterQueryRequest request;
    request.set_queryid(sharedQueryId.getRawValue());

    UnregisterQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    // Call object to store rpc data
    auto* call = new AsyncClientCall<UnregisterQueryReply>;

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
}

bool WorkerRPCClient::unregisterQuery(const std::string& address, SharedQueryId sharedQueryId) {
    NES_DEBUG("WorkerRPCClient::unregisterQuery address={} queryId={}", address, sharedQueryId);

    UnregisterQueryRequest request;
    request.set_queryid(sharedQueryId.getRawValue());

    UnregisterQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->UnregisterQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::unregisterQuery: status ok return success={}", reply.success());
        return reply.success();
    }
    NES_DEBUG(" WorkerRPCClient::unregisterQuery error={}: {}", status.error_code(), status.error_message());
    throw Exceptions::RuntimeException("Error while WorkerRPCClient::unregisterQuery");
}

bool WorkerRPCClient::startQuery(const std::string& address,
                                 SharedQueryId sharedQueryId,
                                 DecomposedQueryPlanId decomposedQueryPlanId) {
    NES_DEBUG("WorkerRPCClient::startQuery address={} shared queryId={} decomposed query plan {}",
              address,
              sharedQueryId,
              decomposedQueryPlanId);

    StartQueryRequest request;
    request.set_sharedqueryid(sharedQueryId.getRawValue());
    request.set_decomposedqueryid(decomposedQueryPlanId.getRawValue());

    StartQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    Status status = workerStub->StartQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::startQuery: status ok return success={}", reply.success());
        return reply.success();
    }
    NES_DEBUG(" WorkerRPCClient::startQuery error={}: {}", status.error_code(), status.error_message());
    throw Exceptions::RuntimeException("Error while WorkerRPCClient::startQuery");
}

void WorkerRPCClient::startQueryAsync(const std::string& address,
                                      SharedQueryId sharedQueryId,
                                      DecomposedQueryPlanId decomposedQueryPlanId,
                                      const CompletionQueuePtr& cq) {
    NES_DEBUG("WorkerRPCClient::startQueryAsync address={} shared queryId={} decomposed queryId={}",
              address,
              sharedQueryId,
              decomposedQueryPlanId);

    StartQueryRequest request;
    request.set_sharedqueryid(sharedQueryId.getRawValue());
    request.set_decomposedqueryid(decomposedQueryPlanId.getRawValue());

    StartQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    // Call object to store rpc data
    auto* call = new AsyncClientCall<StartQueryReply>;

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
}

bool WorkerRPCClient::stopQuery(const std::string& address,
                                SharedQueryId sharedQueryId,
                                DecomposedQueryPlanId decomposedQueryPlanId,
                                Runtime::QueryTerminationType terminationType) {
    NES_DEBUG("WorkerRPCClient::markQueryForStop address={} shared queryId={}", address, sharedQueryId);

    StopQueryRequest request;
    request.set_sharedqueryid(sharedQueryId.getRawValue());
    request.set_decomposedqueryid(decomposedQueryPlanId.getRawValue());
    request.set_queryterminationtype(static_cast<uint64_t>(terminationType));

    StopQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->StopQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::markQueryForStop: status ok return success={}", reply.success());
        return reply.success();
    }
    NES_ERROR(" WorkerRPCClient::markQueryForStop error={}: {}", status.error_code(), status.error_message());
    throw Exceptions::RuntimeException("Error while WorkerRPCClient::markQueryForStop");
}

void WorkerRPCClient::stopQueryAsync(const std::string& address,
                                     SharedQueryId sharedQueryId,
                                     DecomposedQueryPlanId decomposedQueryPlanId,
                                     Runtime::QueryTerminationType terminationType,
                                     const CompletionQueuePtr& cq) {
    NES_DEBUG("WorkerRPCClient::stopQueryAsync address={} shared queryId={}", address, sharedQueryId);

    StopQueryRequest request;
    request.set_sharedqueryid(sharedQueryId.getRawValue());
    request.set_decomposedqueryid(decomposedQueryPlanId.getRawValue());
    request.set_queryterminationtype(static_cast<uint64_t>(terminationType));

    StopQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    // Call object to store rpc data
    auto* call = new AsyncClientCall<StopQueryReply>;

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
}

bool WorkerRPCClient::registerMonitoringPlan(const std::string& address, const Monitoring::MonitoringPlanPtr& plan) {
    NES_DEBUG("WorkerRPCClient: Monitoring request address={}", address);

    MonitoringRegistrationRequest request;
    for (auto metric : plan->getMetricTypes()) {
        request.mutable_metrictypes()->Add(magic_enum::enum_integer(metric));
    }
    ClientContext context;
    MonitoringRegistrationReply reply;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->RegisterMonitoringPlan(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::RequestMonitoringData: status ok");
        return true;
    }
    NES_THROW_RUNTIME_ERROR(" WorkerRPCClient::RequestMonitoringData error=" + std::to_string(status.error_code()) + ": "
                            + status.error_message());
    return false;
}

std::string WorkerRPCClient::requestMonitoringData(const std::string& address) {
    NES_DEBUG("WorkerRPCClient: Monitoring request address={}", address);
    MonitoringDataRequest request;
    ClientContext context;
    MonitoringDataReply reply;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());

    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->GetMonitoringData(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::RequestMonitoringData: metrics received {}", reply.metricsasjson());
        return reply.metricsasjson();
    }
    NES_THROW_RUNTIME_ERROR("WorkerRPCClient::RequestMonitoringData error=" << std::to_string(status.error_code()) << ": "
                                                                            << status.error_message());
}

bool WorkerRPCClient::injectEpochBarrier(uint64_t timestamp, uint64_t queryId, const std::string& address) {
    EpochBarrierNotification request;
    request.set_timestamp(timestamp);
    request.set_queryid(queryId);
    EpochBarrierReply reply;
    ClientContext context;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());

    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->InjectEpochBarrier(&context, request, &reply);
    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::PropagatePunctuation: status ok");
        return true;
    }
    return false;
}

bool WorkerRPCClient::bufferData(const std::string& address, uint64_t querySubPlanId, uint64_t uniqueNetworkSinDescriptorId) {
    NES_DEBUG("WorkerRPCClient::buffering Data on address={}", address);
    BufferRequest request;
    request.set_querysubplanid(querySubPlanId);
    request.set_uniquenetworksinkdescriptorid(uniqueNetworkSinDescriptorId);
    BufferReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->BeginBuffer(&context, request, &reply);
    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::BeginBuffer: status ok return success={}", reply.success());
        return reply.success();
    } else {
        NES_ERROR(" WorkerRPCClient::BeginBuffer "
                  "error={} : {}",
                  status.error_code(),
                  status.error_message());
        throw Exceptions::RuntimeException("Error while WorkerRPCClient::markQueryForStop");
    }
}

bool WorkerRPCClient::updateNetworkSink(const std::string& address,
                                        uint64_t newNodeId,
                                        const std::string& newHostname,
                                        uint32_t newPort,
                                        uint64_t querySubPlanId,
                                        uint64_t uniqueNetworkSinDescriptorId) {
    UpdateNetworkSinkRequest request;
    request.set_newnodeid(newNodeId);
    request.set_newhostname(newHostname);
    request.set_newport(newPort);
    request.set_querysubplanid(querySubPlanId);
    request.set_uniquenetworksinkdescriptorid(uniqueNetworkSinDescriptorId);

    UpdateNetworkSinkReply reply;
    ClientContext context;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->UpdateNetworkSink(&context, request, &reply);
    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::UpdateNetworkSinks: status ok return success={}", reply.success());
        return reply.success();
    } else {
        NES_ERROR(" WorkerRPCClient::UpdateNetworkSinks error={}: {}", status.error_code(), status.error_message());
        throw Exceptions::RuntimeException("Error while WorkerRPCClient::updateNetworkSinks");
    }
}

bool WorkerRPCClient::checkHealth(const std::string& address, std::string healthServiceName) {
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<grpc::health::v1::Health::Stub> workerStub = grpc::health::v1::Health::NewStub(chan);

    grpc::health::v1::HealthCheckRequest request;
    request.set_service(healthServiceName);
    grpc::health::v1::HealthCheckResponse response;
    ClientContext context;
    Status status = workerStub->Check(&context, request, &response);

    if (status.ok()) {
        NES_TRACE("WorkerRPCClient::checkHealth: status ok return success={}", response.status());
        return response.status();
    } else {
        NES_ERROR(" WorkerRPCClient::checkHealth error={}: {}", status.error_code(), status.error_message());
        return response.status();
    }
}

Spatial::DataTypes::Experimental::Waypoint WorkerRPCClient::getWaypoint(const std::string& address) {
    NES_DEBUG("WorkerRPCClient: Requesting location from {}", address)
    ClientContext context;
    GetLocationRequest request;
    GetLocationReply reply;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());

    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->GetLocation(&context, request, &reply);
    if (reply.has_waypoint()) {
        auto waypoint = reply.waypoint();
        auto timestamp = waypoint.timestamp();
        auto geoLocation = waypoint.geolocation();
        //if timestamp is valid, include it in waypoint
        if (timestamp != 0) {
            return {Spatial::DataTypes::Experimental::GeoLocation(geoLocation.lat(), geoLocation.lng()), timestamp};
        }
        //no valid timestamp to include
        return NES::Spatial::DataTypes::Experimental::Waypoint(
            NES::Spatial::DataTypes::Experimental::GeoLocation(geoLocation.lat(), geoLocation.lng()));
    }
    //location is invalid
    return Spatial::DataTypes::Experimental::Waypoint(Spatial::DataTypes::Experimental::Waypoint::invalid());
}

}// namespace NES
