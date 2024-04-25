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

#ifndef NES_NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_GRPC_WORKERSTATISTICRPCCLIENT_HPP_
#define NES_NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_GRPC_WORKERSTATISTICRPCCLIENT_HPP_

#include <Statistics/StatisticRequests.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <WorkerRPCService.pb.h>
#include <grpcpp/grpcpp.h>

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
namespace NES::Statistic {

class WorkerStatisticRPCClient;
using WorkerStatisticRPCClientPtr = std::shared_ptr<WorkerStatisticRPCClient>;

class WorkerStatisticRPCClient {
  public:
    template<typename ReplayType>
    struct AsyncClientCall {
        // Container for the data we expect from the server.
        ReplayType reply;

        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // Storage for the status of the RPC upon completion.
        Status status;

        std::unique_ptr<ClientAsyncResponseReader<ReplayType>> responseReader;
    };

    static WorkerStatisticRPCClientPtr create();

    /**
     * @brief method to probe a statistic
     * @param probeRequest
     * @param gRPCAddress
     * @return Vector of StatisticValues
     */
    std::vector<StatisticValue<>> probeStatistics(const StatisticProbeRequestGRPC& probeRequest, const std::string& gRPCAddress);


  private:
    WorkerStatisticRPCClient() = default;
};

}// namespace NES::Statistic

#endif//NES_NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_GRPC_WORKERSTATISTICRPCCLIENT_HPP_
