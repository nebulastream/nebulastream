#pragma once
#include <CoordinatorEngine/CoordinatorEngine.hpp>
#include <CoordinatorRPCService.grpc.pb.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace NES {
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
    Status RegisterNode(ServerContext* context, const RegisterNodeRequest* request,
                        RegisterNodeReply* reply) override;

    /**
     * @brief RPC Call to unregister a node
     * @param context
     * @param request
     * @param reply
     * @return success
     */
    Status UnregisterNode(ServerContext* context, const UnregisterNodeRequest* request,
                          UnregisterNodeReply* reply) override;

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
    Status AddParent(ServerContext* context, const AddParentRequest* request,
                     AddParentReply* reply) override;

    /**
     * @brief RPC Call to remove parent
     * @param context
     * @param request
     * @param reply
     * @return success
     */
    Status RemoveParent(ServerContext* context, const RemoveParentRequest* request,
                        RemoveParentReply* reply) override;

  private:
    CoordinatorEnginePtr coordinatorEngine;
};
}// namespace NES