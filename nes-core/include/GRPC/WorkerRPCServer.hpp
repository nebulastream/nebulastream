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

#ifndef NES_INCLUDE_GRPC_WORKER_RPC_SERVER_HPP_
#define NES_INCLUDE_GRPC_WORKER_RPC_SERVER_HPP_

#include <CoordinatorRPCService.grpc.pb.h>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace NES {

class MonitoringAgent;
using MonitoringAgentPtr = std::shared_ptr<MonitoringAgent>;

class WorkerRPCServer final : public WorkerRPCService::Service {
  public:
    WorkerRPCServer(Runtime::NodeEnginePtr nodeEngine, MonitoringAgentPtr monitoringAgent);

    Status RegisterQuery(ServerContext* context, const RegisterQueryRequest* request, RegisterQueryReply* reply) override;

    Status UnregisterQuery(ServerContext* context, const UnregisterQueryRequest* request, UnregisterQueryReply* reply) override;

    Status StartQuery(ServerContext* context, const StartQueryRequest* request, StartQueryReply* reply) override;

    Status StopQuery(ServerContext* context, const StopQueryRequest* request, StopQueryReply* reply) override;

    Status RegisterMonitoring(ServerContext* context,
                              const MonitoringRegistrationRequest* request,
                              MonitoringRegistrationReply* reply) override;

    Status GetMonitoringData(ServerContext* context, const MonitoringDataRequest* request, MonitoringDataReply* reply) override;

    Status GetDumpContextInfo(ServerContext* context, const DumpContextRequest* request, DumpContextReply* reply) override;

  private:
    Runtime::NodeEnginePtr nodeEngine;
    MonitoringAgentPtr monitoringAgent;
};

}// namespace NES

#endif// NES_INCLUDE_GRPC_WORKER_RPC_SERVER_HPP_
