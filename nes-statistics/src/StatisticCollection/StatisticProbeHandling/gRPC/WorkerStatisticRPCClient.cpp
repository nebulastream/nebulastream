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
#include <Expressions/ExpressionSerializationUtil.hpp>
#include <Statistics/StatisticRequests.hpp>
#include <Statistics/StatisticValue.hpp>
#include <StatisticCollection/StatisticProbeHandling/gRPC/WorkerStatisticRPCClient.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Statistic {


std::vector<StatisticValue<>>
WorkerStatisticRPCClient::probeStatistics(const StatisticProbeRequestGRPC& probeRequest,
                                 const std::string& gRPCAddress) {
    NES_DEBUG("Requesting statistics from workerId {} at address {}", probeRequest.workerId, gRPCAddress);


    // 1. Building the request
    ClientContext context;
    ProbeStatisticsRequest request;
    request.set_statistichash(probeRequest.statisticHash);
    request.set_startts(probeRequest.startTs.getTime());
    request.set_endts(probeRequest.endTs.getTime());
    request.set_granularity(probeRequest.granularity.getTime());
    auto expressionDetails = request.mutable_expression();
    ExpressionSerializationUtil::serializeExpression(probeRequest.probeExpression.getProbeExpression(), expressionDetails);

    // 2. Sending the request and building a reply
    auto chan = grpc::CreateChannel(gRPCAddress, grpc::InsecureChannelCredentials());
    auto workerStub = WorkerRPCService::NewStub(chan);
    ProbeStatisticsReply reply;
    auto status = workerStub->ProbeStatistics(&context, request, &reply);
    if (status.ok()) {
        // Extracting the statistic values from the reply, if the reply is valid
        std::vector<StatisticValue<>> statisticValues;
        std::transform(reply.statistics().begin(),
                       reply.statistics().end(),
                       std::back_inserter(statisticValues),
                       [](const StatisticReply& statisticValue) {
                           return StatisticValue<>(statisticValue.statisticvalue(),
                                                              statisticValue.startts(),
                                                              statisticValue.endts());
                       });
        return statisticValues;
    } else {
        NES_ERROR("WorkerStatisticRPCClient::probeStatistics error={}: {}", status.error_code(), status.error_message());
        return {};
    }
}


}// namespace NES::Statistic