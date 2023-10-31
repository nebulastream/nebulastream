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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATMANAGER_STATMANAGER_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATMANAGER_STATMANAGER_HPP_

#include <vector>
#include <WorkerRPCService.grpc.pb.h>

namespace NES {

namespace Experimental::Statistics {

class StatCollectorIdentifier;
class ProbeRequestParamObj;
class DeleteRequestParamObj;

class StatManager {
  public:
    double probeStat(StatCollectorIdentifier& statIdentifier);
    void probeStats(ProbeRequestParamObj& probeRequest, ProbeStatReply* stats);
    int64_t deleteStat(DeleteRequestParamObj& deleteRequestParamObj);
  private:

};
}
}

#endif//NES_NES_CORE_INCLUDE_STATISTICS_STATMANAGER_STATMANAGER_HPP_
