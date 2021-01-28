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

#ifndef NES_SRC_GRPC_CALLDATA_HPP_
#define NES_SRC_GRPC_CALLDATA_HPP_

namespace NES {

/**
 * @brief This is taken from https://github.com/grpc/grpc/tree/master/examples/cpp/helloworld
 *  Take in the "service" instance (in this case representing an asynchronous
 * server) and the completion queue "completionQueue" used for asynchronous communication
 * with the gRPC runtime.
 */
class CallData {
  public:
    CallData(WorkerRPCServer::Service* service, grpc_impl::ServerCompletionQueue* cq)
        : service(service), completionQueue(cq), responder(&ctx), status(CREATE) {
        // Invoke the serving logic right away.
        proceed();
    }

    void proceed() {
        if (status == CREATE) {
            NES_DEBUG("RequestInSyncInCreate=" << request.DebugString());
            // Make this instance progress to the PROCESS state.
            status = PROCESS;

            // As part of the initial CREATE state, we *request* that the system
            // start processing requests. In this request, "this" acts are
            // the tag uniquely identifying the request (so that different CallData
            // instances can serve different requests concurrently), in this case
            // the memory address of this CallData instance.
        } else if (status == PROCESS) {
            NES_DEBUG("RequestInSyncInProcees=" << request.DebugString());
            // Spawn a new CallData instance to serve new clients while we process
            // the one for this CallData. The instance will deallocate itself as
            // part of its FINISH state.
            new CallData(service, completionQueue);
            service->RegisterQuery(&ctx, &request, &reply);

            // And we are done! Let the gRPC runtime know we've finished, using the
            // memory address of this instance as the uniquely identifying tag for
            // the event.
            status = FINISH;
            responder.Finish(reply, Status::OK, this);
        } else {
            NES_DEBUG("RequestInSyncInFinish=" << request.DebugString());
            NES_ASSERT(status == FINISH, "RequestInSyncInFinish failed");
            // Once in the FINISH state, deallocate ourselves (CallData).
            delete this;
        }
    }

  private:
    // The means of communication with the gRPC runtime for an asynchronous
    // server.
    WorkerRPCServer::Service* service;
    // The producer-consumer queue where for asynchronous server notifications.
    grpc_impl::ServerCompletionQueue* completionQueue;
    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    ServerContext ctx;

    // What we get from the client.
    RegisterQueryRequest request;
    // What we send back to the client.
    RegisterQueryReply reply;

    // The means to get back to the client.
    grpc_impl::ServerAsyncResponseWriter<RegisterQueryReply> responder;

    // Let's implement a tiny state machine with the following states.
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status;// The current serving state.
};
}// namespace NES
#endif//NES_SRC_GRPC_CALLDATA_HPP_
