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
#include <Expressions/ExpressionSerializationUtil.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Health.grpc.pb.h>
#include <Monitoring/MonitoringPlan.hpp>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Reconfiguration/ReconfigurationMarkerSerializationUtil.hpp>
#include <Statistics/StatisticValue.hpp>
#include <Util/Mobility/GeoLocation.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES {

WorkerRPCClientPtr WorkerRPCClient::create() { return std::make_shared<WorkerRPCClient>(WorkerRPCClient()); }

bool WorkerRPCClient::registerDecomposedQuery(const std::string& address, const DecomposedQueryPlanPtr& decomposedQueryPlan) {
    SharedQueryId sharedQueryId = decomposedQueryPlan->getSharedQueryId();
    auto decomposedQueryId = decomposedQueryPlan->getDecomposedQueryId();
    NES_DEBUG("WorkerRPCClient::registerDecomposedQuery address={} sharedQueryId={} decomposedQueryId = {} ",
              address,
              sharedQueryId,
              decomposedQueryId);

    // wrap the query id and the query operators in the protobuf register query request object.
    RegisterDecomposedQueryRequest request;

    // serialize query plan.
    auto serializedQueryPlan = request.mutable_decomposedqueryplan();
    DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(decomposedQueryPlan, serializedQueryPlan);

    NES_TRACE("WorkerRPCClient:registerDecomposedQuery -> {}", request.DebugString());
    RegisterDecomposedQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->RegisterDecomposedQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::registerDecomposedQuery: status ok return success={}", reply.success());
        return reply.success();
    }
    NES_DEBUG(" WorkerRPCClient::registerDecomposedQuery "
              "error={}: {}",
              status.error_code(),
              status.error_message());
    throw Exceptions::RuntimeException("Error while WorkerRPCClient::registerDecomposedQuery");
}

void WorkerRPCClient::registerDecomposedQueryAsync(const std::string& address,
                                                   const DecomposedQueryPlanPtr& decomposedQueryPlan,
                                                   const CompletionQueuePtr& cq) {
    SharedQueryId sharedQueryId = decomposedQueryPlan->getSharedQueryId();
    DecomposedQueryId decomposedQueryId = decomposedQueryPlan->getDecomposedQueryId();
    NES_DEBUG("WorkerRPCClient::registerDecomposedQueryAsync address={} sharedQueryId={} decomposedQueryId = {}",
              address,
              sharedQueryId,
              decomposedQueryId);

    // wrap the query id and the query operators in the protobuf register query request object.
    RegisterDecomposedQueryRequest request;
    // serialize query plan.
    auto serializableQueryPlan = request.mutable_decomposedqueryplan();
    DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(decomposedQueryPlan, serializableQueryPlan);

    NES_TRACE("WorkerRPCClient:registerDecomposedQuery -> {}", request.DebugString());
    RegisterDecomposedQueryReply reply;
    ClientContext context;

    grpc::ChannelArguments args;
    args.SetInt("test_key", decomposedQueryId.getRawValue());
    std::shared_ptr<::grpc::Channel> channel = grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args);
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(channel);

    // Call object to store rpc data
    auto* call = new AsyncClientCall<RegisterDecomposedQueryReply>;

    // workerStub->PrepareAsyncRegisterQuery() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->responseReader = workerStub->PrepareAsyncRegisterDecomposedQuery(&call->context, request, cq.get());

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
            auto* call = static_cast<AsyncClientCall<RegisterDecomposedQueryReply>*>(got_tag);
            status = call->status.ok();
            delete call;
        } else if (requestClientMode == RpcClientMode::Unregister) {
            auto* call = static_cast<AsyncClientCall<UnregisterDecomposedQueryReply>*>(got_tag);
            status = call->status.ok();
            delete call;
        } else if (requestClientMode == RpcClientMode::Start) {
            auto* call = static_cast<AsyncClientCall<StartDecomposedQueryReply>*>(got_tag);
            status = call->status.ok();
            delete call;
        } else if (requestClientMode == RpcClientMode::Stop) {
            auto* call = static_cast<AsyncClientCall<StopDecomposedQueryReply>*>(got_tag);
            status = call->status.ok();
            delete call;
        } else if (requestClientMode == RpcClientMode::Reconfiguration) {
            auto* call = static_cast<AsyncClientCall<ReconfigurationMarkerReply>*>(got_tag);
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

void WorkerRPCClient::unregisterDecomposedQueryAsync(const std::string& address,
                                                     SharedQueryId sharedQueryId,
                                                     DecomposedQueryId decomposedQueryId,
                                                     const CompletionQueuePtr& cq) {
    NES_DEBUG("WorkerRPCClient::unregisterDecomposedQueryAsync address={} queryId={}", address, sharedQueryId);

    UnregisterDecomposedQueryRequest request;
    request.set_sharedqueryid(sharedQueryId.getRawValue());
    request.set_decomposedqueryid(decomposedQueryId.getRawValue());

    UnregisterDecomposedQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    // Call object to store rpc data
    auto* call = new AsyncClientCall<UnregisterDecomposedQueryReply>;

    // workerStub->PrepareAsyncRegisterQuery() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->responseReader = workerStub->PrepareAsyncUnregisterDecomposedQuery(&call->context, request, cq.get());

    // StartCall initiates the RPC call
    call->responseReader->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call object.
    call->responseReader->Finish(&call->reply, &call->status, (void*) call);
}

bool WorkerRPCClient::unregisterDecomposedQuery(const std::string& address,
                                                SharedQueryId sharedQueryId,
                                                DecomposedQueryId decomposedQueryId) {
    NES_DEBUG("WorkerRPCClient::unregisterDecomposedQuery address={} queryId={}", address, sharedQueryId);

    UnregisterDecomposedQueryRequest request;
    request.set_sharedqueryid(sharedQueryId.getRawValue());
    request.set_decomposedqueryid(decomposedQueryId.getRawValue());

    UnregisterDecomposedQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->UnregisterDecomposedQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::unregisterDecomposedQuery: status ok return success={}", reply.success());
        return reply.success();
    }
    NES_DEBUG(" WorkerRPCClient::unregisterDecomposedQuery error={}: {}", status.error_code(), status.error_message());
    throw Exceptions::RuntimeException("Error while WorkerRPCClient::unregisterDecomposedQuery");
}

bool WorkerRPCClient::startDecomposedQuery(const std::string& address,
                                           SharedQueryId sharedQueryId,
                                           DecomposedQueryId decomposedQueryId) {
    NES_DEBUG("WorkerRPCClient::startDecomposedQuery address={} shared queryId={} decomposed query plan {}",
              address,
              sharedQueryId,
              decomposedQueryId);

    StartDecomposedQueryRequest request;
    request.set_sharedqueryid(sharedQueryId.getRawValue());
    request.set_decomposedqueryid(decomposedQueryId.getRawValue());

    StartDecomposedQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    Status status = workerStub->StartDecomposedQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::startDecomposedQuery: status ok return success={}", reply.success());
        return reply.success();
    }
    NES_DEBUG(" WorkerRPCClient::startDecomposedQuery error={}: {}", status.error_code(), status.error_message());
    throw Exceptions::RuntimeException("Error while WorkerRPCClient::startDecomposedQuery");
}

void WorkerRPCClient::startDecomposedQueryAsync(const std::string& address,
                                                SharedQueryId sharedQueryId,
                                                DecomposedQueryId decomposedQueryId,
                                                const CompletionQueuePtr& cq) {
    NES_DEBUG("WorkerRPCClient::startDecomposedQueryAsync address={} shared queryId={} decomposed queryId={}",
              address,
              sharedQueryId,
              decomposedQueryId);

    StartDecomposedQueryRequest request;
    request.set_sharedqueryid(sharedQueryId.getRawValue());
    request.set_decomposedqueryid(decomposedQueryId.getRawValue());

    StartDecomposedQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    // Call object to store rpc data
    auto* call = new AsyncClientCall<StartDecomposedQueryReply>;

    // workerStub->PrepareAsyncRegisterQuery() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->responseReader = workerStub->PrepareAsyncStartDecomposedQuery(&call->context, request, cq.get());

    // StartCall initiates the RPC call
    call->responseReader->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call object.
    call->responseReader->Finish(&call->reply, &call->status, (void*) call);
}

bool WorkerRPCClient::stopDecomposedQuery(const std::string& address,
                                          SharedQueryId sharedQueryId,
                                          DecomposedQueryId decomposedQueryId,
                                          Runtime::QueryTerminationType terminationType) {
    NES_DEBUG("WorkerRPCClient::markQueryForStop address={} shared queryId={}", address, sharedQueryId);

    StopDecomposedQueryRequest request;
    request.set_sharedqueryid(sharedQueryId.getRawValue());
    request.set_decomposedqueryid(decomposedQueryId.getRawValue());
    request.set_queryterminationtype(static_cast<uint64_t>(terminationType));

    StopDecomposedQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->StopDecomposedQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::markQueryForStop: status ok return success={}", reply.success());
        return reply.success();
    }
    NES_ERROR(" WorkerRPCClient::markQueryForStop error={}: {}", status.error_code(), status.error_message());
    throw Exceptions::RuntimeException("Error while WorkerRPCClient::markQueryForStop");
}

void WorkerRPCClient::stopDecomposedQueryAsync(const std::string& address,
                                               SharedQueryId sharedQueryId,
                                               DecomposedQueryId decomposedQueryId,
                                               Runtime::QueryTerminationType terminationType,
                                               const CompletionQueuePtr& cq) {
    NES_DEBUG("WorkerRPCClient::stopDecomposedQueryAsync address={} shared queryId={}", address, sharedQueryId);

    StopDecomposedQueryRequest request;
    request.set_sharedqueryid(sharedQueryId.getRawValue());
    request.set_decomposedqueryid(decomposedQueryId.getRawValue());
    request.set_queryterminationtype(static_cast<uint64_t>(terminationType));

    StopDecomposedQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    // Call object to store rpc data
    auto* call = new AsyncClientCall<StopDecomposedQueryReply>;

    // workerStub->PrepareAsyncRegisterQuery() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->responseReader = workerStub->PrepareAsyncStopDecomposedQuery(&call->context, request, cq.get());

    // StartCall initiates the RPC call
    call->responseReader->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call object.
    call->responseReader->Finish(&call->reply, &call->status, (void*) call);
}

void WorkerRPCClient::addReconfigurationMarker(const std::string& address,
                                               const NES::SharedQueryId& sharedQueryId,
                                               const NES::DecomposedQueryId& decomposedQueryId,
                                               const NES::ReconfigurationMarkerPtr& reconfigurationMarker,
                                               const NES::CompletionQueuePtr& cq) {

    ReconfigurationMarkerRequest request;
    request.set_decomposedqueryid(decomposedQueryId.getRawValue());
    request.set_sharedqueryid(sharedQueryId.getRawValue());
    auto serializableReconfigurationMarker = request.mutable_serializablereconfigurationmarker();
    ReconfigurationMarkerSerializationUtil::serialize(reconfigurationMarker, *serializableReconfigurationMarker);

    ClientContext context;
    // Call object to store rpc data
    auto* call = new AsyncClientCall<ReconfigurationMarkerReply>;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    // workerStub->PrepareAsyncRegisterQuery() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->responseReader = workerStub->PrepareAsyncAddReconfigurationMarker(&call->context, request, cq.get());

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
    request.set_sharedqueryid(queryId);
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

bool WorkerRPCClient::bufferData(const std::string& address,
                                 DecomposedQueryId decomposedQueryId,
                                 uint64_t uniqueNetworkSinDescriptorId) {
    NES_DEBUG("WorkerRPCClient::buffering Data on address={}", address);
    BufferRequest request;
    request.set_decomposedqueryid(decomposedQueryId.getRawValue());
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
                                        DecomposedQueryId decomposedQueryId,
                                        uint64_t uniqueNetworkSinDescriptorId) {
    UpdateNetworkSinkRequest request;
    request.set_newnodeid(newNodeId);
    request.set_newhostname(newHostname);
    request.set_newport(newPort);
    request.set_decomposedqueryid(decomposedQueryId.getRawValue());
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

void WorkerRPCClient::startBufferingAsync(std::string address, const CompletionQueuePtr& cq, WorkerId newParent) {
    StartBufferingRequest request;
    request.set_parent(newParent.getRawValue());
    StartBufferingReply reply;
    ClientContext context;

    std::shared_ptr<Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    // Call object to store rpc data
    auto* call = new AsyncClientCall<StartBufferingReply>;

    call->responseReader = workerStub->PrepareAsyncStartBufferingOnAllSinks(&call->context, request, cq.get());

    call->responseReader->StartCall();

    call->responseReader->Finish(&call->reply, &call->status, (void*) call);
}
}// namespace NES
