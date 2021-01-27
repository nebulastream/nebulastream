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

struct AsyncClientCall {
    // Container for the data we expect from the server.
    RegisterQueryReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // Storage for the status of the RPC upon completion.
    Status status;

    std::unique_ptr<ClientAsyncResponseReader<RegisterQueryReply>> response_reader;
};

// Loop while listening for completed responses.
// Prints out the response from the server.
void WorkerRPCClient::AsyncCompleteRpc() {
    void* got_tag;
    bool ok = false;

    // Block until the next result is available in the completion queue "cq".
    NES_DEBUG("initalized async ");
    while (cq.Next(&got_tag, &ok)) {
        NES_DEBUG("Async got something");
        // The tag in this example is the memory location of the call object
        AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

        // Verify that the request was completed successfully. Note that "ok"
        // corresponds solely to the request for updates introduced by Finish().
        GPR_ASSERT(ok);

        if (call->status.ok())
            std::cout << "Greeter received: " << call->reply.DebugString() << std::endl;
        else
            std::cout << "RPC failed" << std::endl;

        // Once we're complete, deallocate the call object.
        delete call;
    }
}

WorkerRPCClient::WorkerRPCClient() {
    NES_DEBUG("WorkerRPCClient()");
//    waitingThread = std::thread(&WorkerRPCClient::AsyncCompleteRpc, this);
}

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

bool WorkerRPCClient::registerQueryAsync(std::string address, QueryPlanPtr queryPlan) {
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

    std::shared_ptr<::grpc::Channel> channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(channel);

    // Call object to store rpc data
    AsyncClientCall* call = new AsyncClientCall;
    call->response_reader = workerStub->PrepareAsyncRegisterQuery(&call->context, request, &cq);

    call->response_reader->StartCall();

    call->response_reader->Finish(&call->reply, &call->status, (void*) call);
    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    while (cq.Next(&got_tag, &ok)) {
        // The tag in this example is the memory location of the call object
        AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

        // Verify that the request was completed successfully. Note that "ok"
        // corresponds solely to the request for updates introduced by Finish().
        GPR_ASSERT(ok);

        if (call->status.ok())
        {
            std::cout << "Greeter received: " << call->reply.DebugString() << std::endl;
            return true;
        }
        else
        {
            std::cout << "RPC failed" << std::endl;
            return false;
        }

        // Once we're complete, deallocate the call object.
        delete call;
    }

    // Act upon the status of the actual RPC.
    if (call->status.ok()) {
        return true;
    } else {
        return false;
    }

    //
    //    // stub_->PrepareAsyncSayHello() creates an RPC object, returning
    //    // an instance to store in "call" but does not actually start the RPC
    //    // Because we are using the asynchronous API, we need to hold on to
    //    // the "call" instance in order to get updates on the ongoing RPC.
    //    std::unique_ptr<ClientAsyncResponseReader<RegisterQueryReply>> rpc(
    //        workerStub->PrepareAsyncRegisterQuery(&context, request, &cq));
    //
    //    // StartCall initiates the RPC call
    //    rpc->StartCall();
    //
    //    // Storage for the status of the RPC upon completion.
    ////    Status status;
    //
    //    // Request that, upon completion of the RPC, "reply" be updated with the
    //    // server's response; "status" with the indication of whether the operation
    //    // was successful. Tag the request with the integer 1.
    //    rpc->Finish(&reply, &status, (void*) 1);

    //
    //    void* got_tag;
    //    bool ok = false;
    //    // Block until the next result is available in the completion queue "cq".
    //    while (cq.Next(&got_tag, &ok)) {
    //        // The tag in this example is the memory location of the call object
    //
    //        // Verify that the request was completed successfully. Note that "ok"
    //        // corresponds solely to the request for updates introduced by Finish().
    //        GPR_ASSERT(ok);
    //
    //
    //        NES_DEBUG("CQ got an element");
    //    }
    //
    //    // Block until the next result is available in the completion queue "cq".
    //    // The return value of Next should always be checked. This return value
    //    // tells us whether there is any kind of event or the cq_ is shutting down.
    //    GPR_ASSERT(cq.Next(&got_tag, &ok));
    //
    //    // Verify that the result from "cq" corresponds, by its tag, our previous
    //    // request.
    //    GPR_ASSERT(got_tag == (void*) 1);
    //    // ... and that the request was completed successfully. Note that "ok"
    //    // corresponds solely to the request for updates introduced by Finish().
    //    GPR_ASSERT(ok);
    //
    //    if (status.ok()) {
    //        NES_DEBUG("WorkerRPCClient::registerQuery: status ok return success=" << reply.success());
    //        return reply.success();
    //    } else {
    //        NES_DEBUG(" WorkerRPCClient::registerQuery error="
    //                  << status.error_code() << ": " << status.error_message());
    //        throw Exception("Error while WorkerRPCClient::registerQuery");
    //    }
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

SchemaPtr WorkerRPCClient::requestMonitoringData(const std::string& address, MonitoringPlanPtr plan,
                                                 NodeEngine::TupleBuffer& buf) {
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
    return nullptr;
}

}// namespace NES
