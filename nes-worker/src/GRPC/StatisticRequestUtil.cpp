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

#include <GRPC/StatisticRequestUtil.hpp>
#include <StatisticDeleteRequest.pb.h>
#include <StatisticProbeRequest.pb.h>
#include <Statistics/Requests/StatisticDeleteRequest.hpp>
#include <Statistics/Requests/StatisticProbeRequest.hpp>

namespace NES {

void StatisticRequestUtil::serializeProbeRequest(const Experimental::Statistics::StatisticProbeRequest& statisticProbeRequest,
                                            GRPCStatisticProbeRequest* grpcProbeRequest) {
    grpcProbeRequest->set_logicalsourcename(statisticProbeRequest.getLogicalSourceName());
    grpcProbeRequest->set_fieldname(statisticProbeRequest.getFieldName());
    grpcProbeRequest->set_statisticcollectortype((int32_t) statisticProbeRequest.getStatisticCollectorType());
    for (const auto& physicalSourceName : statisticProbeRequest.getPhysicalSourceNames()) {
        grpcProbeRequest->add_physicalsourcenames(physicalSourceName);
    }
    grpcProbeRequest->set_probeexpression(statisticProbeRequest.getProbeExpression());
    grpcProbeRequest->set_starttime(statisticProbeRequest.getStartTime());
    grpcProbeRequest->set_endtime(statisticProbeRequest.getEndTime());
    grpcProbeRequest->set_merge(statisticProbeRequest.getMerge());
}

Experimental::Statistics::StatisticProbeRequest&
StatisticRequestUtil::deserializeProbeRequest(const GRPCStatisticProbeRequest* grpcProbeRequest) {
    std::vector<std::string> physicalSourceNames;
    for (const auto& physicalSourceName : grpcProbeRequest->physicalsourcenames()) {
        physicalSourceNames.push_back(physicalSourceName);
    }

    auto probeRequest = new Experimental::Statistics::StatisticProbeRequest(
        grpcProbeRequest->logicalsourcename(),
        grpcProbeRequest->fieldname(),
        (Experimental::Statistics::StatisticCollectorType) grpcProbeRequest->statisticcollectortype(),
        grpcProbeRequest->probeexpression(),
        physicalSourceNames,
        grpcProbeRequest->starttime(),
        grpcProbeRequest->endtime(),
        grpcProbeRequest->merge());

    return *probeRequest;
};

void StatisticRequestUtil::serializeDeleteRequest(const Experimental::Statistics::StatisticDeleteRequest& statisticDeleteRequest,
                                             GRPCStatisticDeleteRequest* grpcDeleteRequest) {
    grpcDeleteRequest->set_logicalsourcename(statisticDeleteRequest.getLogicalSourceName());
    grpcDeleteRequest->set_fieldname(statisticDeleteRequest.getFieldName());
    grpcDeleteRequest->set_statisticcollectortype((int32_t) statisticDeleteRequest.getStatisticCollectorType());
    grpcDeleteRequest->set_endtime(statisticDeleteRequest.getEndTime());
}

Experimental::Statistics::StatisticDeleteRequest&
StatisticRequestUtil::deserializeDeleteRequest(const GRPCStatisticDeleteRequest* grpcDeleteRequest) {
    auto deleteRequest = new Experimental::Statistics::StatisticDeleteRequest(
        grpcDeleteRequest->logicalsourcename(),
        grpcDeleteRequest->fieldname(),
        (Experimental::Statistics::StatisticCollectorType) grpcDeleteRequest->statisticcollectortype(),
        grpcDeleteRequest->endtime());

    return *deleteRequest;
}

}// namespace NES
