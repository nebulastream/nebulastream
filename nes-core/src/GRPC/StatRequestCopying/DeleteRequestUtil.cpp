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

#include <GRPC/StatRequestCopying/DeleteRequestUtil.hpp>
#include <Statistics/Requests/DeleteRequestParamObj.hpp>
#include <DeleteRequestParamObj.pb.h>

namespace NES {

void DeleteRequestUtil::copyDeleteRequest(const Experimental::Statistics::DeleteRequestParamObj& deleteRequestParamObj,
                                          DeleteStat* probeRequest) {
    probeRequest->set_logicalsourcename(deleteRequestParamObj.getLogicalSourceName());
    probeRequest->set_fieldname(deleteRequestParamObj.getFieldName());
    probeRequest->set_statcollectortype((uint32_t) deleteRequestParamObj.getStatCollectorType());
    probeRequest->set_endtime(deleteRequestParamObj.getEndTime());
}
}