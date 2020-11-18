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

#include <CoordinatorRPCService.grpc.pb.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace NES {

class CoordinatorEngine;
typedef std::shared_ptr<CoordinatorEngine> CoordinatorEnginePtr;

class CoordinatorRPCServer final : public CoordinatorRPCService::Service {
  public:
    CoordinatorRPCServer(CoordinatorEnginePtr coordinatorEngine);

    /**
     * @brief RPC Call to register a node
     * @param context
     * @param request
     * @param reply
     * @return success
     */
    Status RegisterNode(ServerContext* context, const RegisterNodeRequest* request, RegisterNodeReply* reply) override;

    /**
     * @brief RPC Call to unregister a node
     * @param context
     * @param request
     * @param reply
     * @return success
     */
    Status UnregisterNode(ServerContext* context, const UnregisterNodeRequest* request, UnregisterNodeReply* reply) override;

    /**
     * @brief RPC Call to register physical stream
     * @param context
     * @param request
     * @param reply
     * @return success
     */
    Status RegisterPhysicalStream(ServerContext* context, const RegisterPhysicalStreamRequest* request,
                                  RegisterPhysicalStreamReply* reply) override;

    /**
     * @brief RPC Call to unregister physical stream
     * @param context
     * @param request
     * @param reply
     * @return success
     */
    Status UnregisterPhysicalStream(ServerContext* context, const UnregisterPhysicalStreamRequest* request,
                                    UnregisterPhysicalStreamReply* reply) override;

    /**
     * @brief RPC Call to register logical stream
     * @param context
     * @param request
     * @param reply
     * @return success
     */
    Status RegisterLogicalStream(ServerContext* context, const RegisterLogicalStreamRequest* request,
                                 RegisterLogicalStreamReply* reply) override;

    /**
     * @brief RPC Call to unregister logical stream
     * @param context
     * @param request
     * @param reply
     * @return success
     */
    Status UnregisterLogicalStream(ServerContext* context, const UnregisterLogicalStreamRequest* request,
                                   UnregisterLogicalStreamReply* reply) override;

    /**
     * @brief RPC Call to add parent
     * @param context
     * @param request
     * @param reply
     * @return success
     */
    Status AddParent(ServerContext* context, const AddParentRequest* request, AddParentReply* reply) override;

    /**
     * @brief RPC Call to remove parent
     * @param context
     * @param request
     * @param reply
     * @return success
     */
    Status RemoveParent(ServerContext* context, const RemoveParentRequest* request, RemoveParentReply* reply) override;

  private:
    CoordinatorEnginePtr coordinatorEngine;
};
}// namespace NES