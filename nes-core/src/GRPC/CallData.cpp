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
#include <GRPC/CallData.hpp>
#include <GRPC/WorkerRPCServer.hpp>
#include <Util/Logger.hpp>

namespace NES {

CallData::CallData(WorkerRPCServer& service, grpc::ServerCompletionQueue* cq)
    : service(service), completionQueue(cq), status(CREATE) {
    // Invoke the serving logic right away.
}

void CallData::proceed() {
    // What we get from the client.
    RegisterQueryRequest request;
    //
    //    // What we send back to the client.
    RegisterQueryReply reply;

    ServerContext ctx;
    grpc::ServerAsyncResponseWriter<RegisterQueryReply> responder(&ctx);

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
        service.RegisterQuery(&ctx, &request, &reply);

        // And we are done! Let the gRPC Runtime know we've finished, using the
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

}// namespace NES