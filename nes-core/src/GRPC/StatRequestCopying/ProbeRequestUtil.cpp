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

#include <GRPC/StatRequestCopying/ProbeRequestUtil.hpp>
#include <Statistics/Requests/ProbeRequestParamObj.hpp>
#include <ProbeRequestParamObj.pb.h>

namespace NES {

void ProbeRequestUtil::copyProbeRequest(const Experimental::Statistics::ProbeRequestParamObj& probeRequestParamObj,
                             ProbeStat* probeRequest) {
    probeRequest->set_logicalsourcename(probeRequestParamObj.getLogicalSourceName());
    probeRequest->set_fieldname(probeRequestParamObj.getFieldName());
    probeRequest->set_statcollectortype((uint32_t) probeRequestParamObj.getStatCollectorType());
    for (auto physicalSourceName : probeRequestParamObj.getPhysicalSourceNames()){
        probeRequest->add_physicalsourcenames(physicalSourceName);
    }
    probeRequest->set_expression(probeRequestParamObj.getProbeExpression());
    probeRequest->set_starttime(probeRequestParamObj.getStartTime());
    probeRequest->set_endtime(probeRequestParamObj.getEndTime());
    probeRequest->set_merge(probeRequestParamObj.getMerge());
}
}
