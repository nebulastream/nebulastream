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

#ifndef NES_WORKER_INCLUDE_GRPC_WORKERRPCSERVER_HPP_
#define NES_WORKER_INCLUDE_GRPC_WORKERRPCSERVER_HPP_

#include <CoordinatorRPCService.grpc.pb.h>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace NES {

namespace Monitoring {
class MonitoringAgent;
using MonitoringAgentPtr = std::shared_ptr<MonitoringAgent>;
}// namespace Monitoring

namespace Statistic {
class StatisticManager;
using StatisticManagerPtr = std::shared_ptr<StatisticManager>;
}// namespace Statistic

namespace Spatial::Mobility::Experimental {
class LocationProvider;
using LocationProviderPtr = std::shared_ptr<LocationProvider>;

class ReconnectSchedulePredictor;
using ReconnectSchedulePredictorPtr = std::shared_ptr<ReconnectSchedulePredictor>;
}// namespace Spatial::Mobility::Experimental

class WorkerRPCServer final : public WorkerRPCService::Service {
  public:
    WorkerRPCServer(Runtime::NodeEnginePtr nodeEngine,
                    Monitoring::MonitoringAgentPtr monitoringAgent,
                    NES::Spatial::Mobility::Experimental::LocationProviderPtr locationProvider,
                    NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictorPtr trajectoryPredictor);

    Status RegisterDecomposedQuery(ServerContext* context,
                                   const RegisterDecomposedQueryRequest* request,
                                   RegisterDecomposedQueryReply* reply) override;

    Status UnregisterDecomposedQuery(ServerContext* context,
                                     const UnregisterDecomposedQueryRequest* request,
                                     UnregisterDecomposedQueryReply* reply) override;

    Status StartDecomposedQuery(ServerContext* context,
                                const StartDecomposedQueryRequest* request,
                                StartDecomposedQueryReply* reply) override;

    Status StopDecomposedQuery(ServerContext* context,
                               const StopDecomposedQueryRequest* request,
                               StopDecomposedQueryReply* reply) override;

    Status AddReconfigurationMarker(::grpc::ServerContext* context,
                                    const ::ReconfigurationMarkerRequest* request,
                                    ::ReconfigurationMarkerReply* response) override;

    Status
    RegisterMonitoringPlan(ServerContext*, const MonitoringRegistrationRequest* request, MonitoringRegistrationReply*) override;

    Status GetMonitoringData(ServerContext* context, const MonitoringDataRequest* request, MonitoringDataReply* reply) override;

    Status BeginBuffer(ServerContext* context, const BufferRequest* request, BufferReply* reply) override;

    Status StartBufferingOnAllSinks(ServerContext* context, const StartBufferingRequest* request, StartBufferingReply* reply) override;

    Status UpdateNetworkSink(ServerContext*, const UpdateNetworkSinkRequest* request, UpdateNetworkSinkReply* reply) override;

    Status GetLocation(ServerContext*, const GetLocationRequest* request, GetLocationReply* reply) override;

    Status ProbeStatistics(ServerContext*, const ProbeStatisticsRequest* request, ProbeStatisticsReply* reply) override;

  private:
    Runtime::NodeEnginePtr nodeEngine;
    Statistic::StatisticManagerPtr statisticManager;
    Monitoring::MonitoringAgentPtr monitoringAgent;
    NES::Spatial::Mobility::Experimental::LocationProviderPtr locationProvider;
    NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictorPtr trajectoryPredictor;
};

}// namespace NES

#endif// NES_WORKER_INCLUDE_GRPC_WORKERRPCSERVER_HPP_
