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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATISTICMANAGER_STATISTICMANAGER_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATISTICMANAGER_STATISTICMANAGER_HPP_

#include <Identifiers.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <Util/StatisticCollectorType.hpp>
#include <vector>

namespace NES::Experimental::Statistics {

class StatisticCollectorIdentifier;

class StatisticCollectorStorage;
using StatisticCollectorStoragePtr = std::shared_ptr<StatisticCollectorStorage>;

/**
* @brief The StatisticManager is a class that resides on workers and that has access to a StatisticCollectorStorage. Further, the StatisticManager
* is a member of the WorkerRPCServer. When the WorkerRPCServer receives any Statistic request, it utilizes the StatisticManager
* to generate replies for the received requests.
*/
class StatisticManager {
  public:
    /**
     * @brief the default constructor of the StatisticManager
     */
    StatisticManager();

    /**
     * @return
     */
    StatisticCollectorStoragePtr getStatisticCollectorStorage();

    /**
     * @brief receives both a request and reply object and fills the reply object with the desired allStatistics specified by the
     * request
     * @param logicalSourceName
     * @param allPhysicalSources
     * @param workerId the id of the worker on which we are working
     * @param fieldName
     * @param statisticCollectorType
     * @param allStatistics a reply object will be filled with the desired/specified allStatistics
     */
    void probeStatistic(const std::string& logicalSourceName,
                        const std::vector<std::string>& allPhysicalSources,
                        WorkerId workerId,
                        const std::string& fieldName,
                        StatisticCollectorType statisticCollectorType,
                        ProbeStatisticReply* allStatistics);

    /**
     * @brief receives a StatisticDeleteRequest and removes the according entries from the StatisticCollectorStorage
     * @param logicalSourceName
     * @param allPhycialSourceNames
     * @param workerId the id of the worker on which we are working
     * @param fieldName
     * @param statisticCollectorType
     * @return returns true on success, otherwise false
     */
    bool deleteStatistic(const std::string& logicalSourceName,
                         const std::vector<std::string>& allPhycialSourceNames,
                         WorkerId nodeID,
                         const std::string& fieldName,
                         StatisticCollectorType statisticCollectorType);

  private:
    StatisticCollectorStoragePtr statisticCollectorStorage;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_CORE_INCLUDE_STATISTICS_STATISTICMANAGER_STATISTICMANAGER_HPP_
