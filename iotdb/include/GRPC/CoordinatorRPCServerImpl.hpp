#pragma once
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <GRPC/Coordinator.grpc.pb.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

class CoordinatorRPCServerImpl final : public CoordinatorService::Service {

    Status RegisterNode(ServerContext* context, const RegisterNodeRequest* request,
                        RegisterNodeReply* reply) override;

    Status UnregisterNode(ServerContext* context, const UnregisterNodeRequest* request,
                          UnregisterNodeReply* reply) override;

    Status RegisterPhysicalStream(ServerContext* context, const RegisterPhysicalStreamRequest* request,
                                  RegisterPhysicalStreamReply* reply) override;

    Status UnregisterPhysicalStream(ServerContext* context, const UnregisterPhysicalStreamRequest* request,
                                    UnregisterPhysicalStreamReply* reply) override;

    Status RegisterLogicalStream(ServerContext* context, const RegisterLogicalStreamRequest* request,
                                 RegisterLogicalStreamReply* reply) override;

    Status UnregisterLogicalStream(ServerContext* context, const UnregisterLogicalStreamRequest* request,
                                   UnregisterLogicalStreamReply* reply) override;

    Status AddParent(ServerContext* context, const AddParentRequest* request,
                     AddParentReply* reply) override;

    Status RemoveParent(ServerContext* context, const RemoveParentRequest* request,
                        RemoveParentReply* reply) override;

};