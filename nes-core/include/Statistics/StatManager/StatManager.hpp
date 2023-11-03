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

#include <WorkerRPCService.grpc.pb.h>
#include <vector>

namespace NES::Experimental::Statistics {

class StatCollectorIdentifier;
class StatProbeRequest;
class StatDeleteRequest;

/**
 * @brief The StatManager is a class that resides on workers and that has access to a StatCollectorStorage. Further, the StatManager
 * is a member of the WorkerRPCServer. When the WorkerRPCServer receives any Statistic request, it utilizes the StatManager
 * to generate replies for the received requests.
 */
class StatManager {
  public:
    /**
     * @brief the default constructor of the StatManager
     */
    StatManager();

    /**
     * @brief receives a single statCollectorIdentifier and will probe the StatCollectorStorage for the specified StatCollector
     * and then extract the specified statistic from it
     * @param statCollectorIdentifier a unique Identifier which can be hashed and used to identify StatCollectors
     * @return a single statistic
     */
    double probeStat(StatCollectorIdentifier& statCollectorIdentifier);

    /**
     * @brief receives both a request and reply object and fills the reply object with the desired statistics specified by the
     * request
     * @param probeRequest a porbeRequest, specifying everything about the desired statistic(s)
     * @param stats a reply object will be filled with the desired/specified statistics
     */
    void probeStats(StatProbeRequest& probeRequest, ProbeStatReply* stats);

    /**
     * @brief receives a deleteRequestParamObj and removes the according entries from the StatCollectorStorage
     * @param deleteRequestParamObj a delete request, specifying one or more StatCollector(s) to be deleted from the node local
     * StatCollectorStorage
     * @return returns true on success, otherwise false
     */
    bool deleteStat(StatDeleteRequest& deleteRequestParamObj);

  private:
    // ToDo: Add StatCollectorStorage Issue: 4234
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_CORE_INCLUDE_STATISTICS_STATMANAGER_STATMANAGER_HPP_
