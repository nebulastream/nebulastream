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

#ifndef NES_INCLUDE_GRPC_CALL_DATA_HPP_
#define NES_INCLUDE_GRPC_CALL_DATA_HPP_

namespace grpc {
class ServerCompletionQueue;
}

namespace NES {
class WorkerRPCServer;
/**
 * @brief This is taken from https://github.com/grpc/grpc/tree/master/examples/cpp/helloworld
 *  Take in the "service" instance (in this case representing an asynchronous
 * server) and the completion queue "completionQueue" used for asynchronous communication
 * with the gRPC Runtime.
 */
class CallData {
  public:
    /**
     * @brief Constructor for the Call Data
     * @param service server to listen on
     * @param cq queue to listen on
     */
    CallData(WorkerRPCServer& service, grpc::ServerCompletionQueue* cq);

    /**
    * @brief Run method to process the call data through it different stages
    */
    void proceed();

  private:
    // The means of communication with the gRPC Runtime for an asynchronous
    // server.
    WorkerRPCServer& service;

    // The producer-consumer queue where for asynchronous server notifications.
    [[maybe_unused]] grpc::ServerCompletionQueue* completionQueue;

    // Let's implement a tiny state machine with the following states.
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status;// The current serving state.
};

}// namespace NES
#endif  // NES_INCLUDE_GRPC_CALL_DATA_HPP_
