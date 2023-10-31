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
#include <WorkerRPCService.pb.h>
#include <Statistics/Requests/DeleteRequestParamObj.hpp>
#include <DeleteRequestParamObj.pb.h>

namespace NES {

void DeleteRequestUtil::packDeleteRequest(const Experimental::Statistics::DeleteRequestParamObj& deleteRequestParamObj,
                                          DeleteStat* deleteRequest) {
    deleteRequest->set_logicalsourcename(deleteRequestParamObj.getLogicalSourceName());
    deleteRequest->set_fieldname(deleteRequestParamObj.getFieldName());
    deleteRequest->set_statcollectortype((uint32_t) deleteRequestParamObj.getStatCollectorType());
    deleteRequest->set_endtime(deleteRequestParamObj.getEndTime());
}

Experimental::Statistics::DeleteRequestParamObj DeleteRequestUtil::unpackDeleteRequest(const DeleteStat* deleteRequest) {

    return Experimental::Statistics::DeleteRequestParamObj(deleteRequest->logicalsourcename(),
                                                           deleteRequest->fieldname(),
                                                           (Experimental::Statistics::StatCollectorType) deleteRequest->statcollectortype(),
                                                           deleteRequest->endtime());
}

}