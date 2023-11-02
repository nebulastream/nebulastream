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

#include "GRPC/StatRequestUtil.hpp"
#include "DeleteRequestParamObj.pb.h"
#include "ProbeRequestParamObj.pb.h"
#include "Statistics/Requests/StatDeleteRequest.hpp"
#include "Statistics/Requests/StatProbeRequest.hpp"

namespace NES {

void StatRequestUtil::serializeProbeRequest(const Experimental::Statistics::StatProbeRequest& statProbeRequest,
                                            GRPCStatProbeRequest* grpcProbeRequest) {
    grpcProbeRequest->set_logicalsourcename(statProbeRequest.getLogicalSourceName());
    grpcProbeRequest->set_fieldname(statProbeRequest.getFieldName());
    grpcProbeRequest->set_statcollectortype((uint32_t) statProbeRequest.getStatCollectorType());
    for (const auto& physicalSourceName : statProbeRequest.getPhysicalSourceNames()) {
        grpcProbeRequest->add_physicalsourcenames(physicalSourceName);
    }
    grpcProbeRequest->set_expression(statProbeRequest.getProbeExpression());
    grpcProbeRequest->set_starttime(statProbeRequest.getStartTime());
    grpcProbeRequest->set_endtime(statProbeRequest.getEndTime());
    grpcProbeRequest->set_merge(statProbeRequest.getMerge());
}

void StatRequestUtil::serializeDeleteRequest(const Experimental::Statistics::StatDeleteRequest& statDeleteRequest,
                                             GRPCStatDeleteRequest* grpcDeleteRequest) {
    grpcDeleteRequest->set_logicalsourcename(statDeleteRequest.getLogicalSourceName());
    grpcDeleteRequest->set_fieldname(statDeleteRequest.getFieldName());
    grpcDeleteRequest->set_statcollectortype((uint32_t) statDeleteRequest.getStatCollectorType());
    grpcDeleteRequest->set_endtime(statDeleteRequest.getEndTime());
}
}// namespace NES
