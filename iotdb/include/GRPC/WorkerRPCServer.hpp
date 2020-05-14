#pragma once
#include <NodeEngine/NodeEngine.hpp>
#include <Services/CoordinatorService.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace NES {
class WorkerRPCServer final : public WorkerRPCService::Service {
  public:
    WorkerRPCServer(NodeEnginePtr nodeEngine);

    Status DeployQuery(ServerContext* context, const DeployQueryRequest* request,
                       DeployQueryReply* reply) override;

    Status UndeployQuery(ServerContext* context, const UndeployQueryRequest* request,
                         UndeployQueryReply* reply) override;

    Status RegisterQuery(ServerContext* context, const RegisterQueryRequest* request,
                         RegisterQueryReply* reply) override;

    Status UnregisterQuery(ServerContext* context, const UnregisterQueryRequest* request,
                           UnregisterQueryReply* reply) override;

    Status StartQuery(ServerContext* context, const StartQueryRequest* request,
                      StartQueryReply* reply) override;

    Status StopQuery(ServerContext* context, const StopQueryRequest* request,
                     StopQueryReply* reply) override;

  private:
    NodeEnginePtr nodeEngine;
};
}// namespace NES