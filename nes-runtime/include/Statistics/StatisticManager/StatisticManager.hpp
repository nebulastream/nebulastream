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
#include <Operators/Expressions/ExpressionNode.hpp>
#include <Util/StatisticCollectorType.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <vector>

namespace NES {

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

namespace Experimental::Statistics {

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
     * @return returns the statisticCollectorStorage of the StatisticManager
     */
    StatisticCollectorStoragePtr getStatisticCollectorStorage();

    /**
     * @brief receives both a request and reply object and fills the reply object with the desired allStatistics specified by the
     * request
     * @param logicalSourceName the logicalSourceName over which the statistic that we want was built
     * @param fieldName the fieldName over which the statistic that we want was built
     * @param statisticCollectorType the type of statistic that is to be probed
     * @param probeExpression the expression defining the statistic that we want
     * @param allPhysicalSources the physicalSourceNames belonging to the logicalSource that are on the local node over which the statistic
     * that we want was built
     * @param startTime the time at which the statistic that we want was created
     * @param endTime the time at which the statistic that we want was completed
     * @param allStatistics a reply object will be filled with the desired/specified allStatistics
     */
    void probeStatistic(const std::string& logicalSourceName,
                        const std::string& fieldName,
                        StatisticCollectorType statisticCollectorType,
                        ExpressionNodePtr& probeExpression,
                        const std::vector<std::string>& allPhysicalSources,
                        const time_t startTime,
                        const time_t endTime,
                        ProbeStatisticReply* allStatistics);

    /**
     * @brief receives a StatisticDeleteRequest and removes the according entries from the StatisticCollectorStorage
     * @param logicalSourceName the logicalSourceName over which the statistic that we want to delete was built
     * @param fieldName the fieldName over which the statistic that we want to delete was built
     * @param allPhycialSourceNames the physicalSourceNames belonging to the the logicalSourceName on the local node
     * that whose statistics we want to delete
     * @param startTime the time at which the statistic that we want to delete was created
     * @param endTime the time at which the statistic that we want to delete was completed
     * @param statisticCollectorType the type of statistic that is to be deleted
     * @return returns true on success, otherwise false
     */
    bool deleteStatistic(const std::string& logicalSourceName,
                         const std::vector<std::string>& allPhycialSourceNames,
                         const std::string& fieldName,
                         const time_t startTime,
                         const time_t endTime,
                         StatisticCollectorType statisticCollectorType);

  private:
    StatisticCollectorStoragePtr statisticCollectorStorage;
};
}// namespace Experimental::Statistics
}// namespace NES

#endif//NES_NES_CORE_INCLUDE_STATISTICS_STATISTICMANAGER_STATISTICMANAGER_HPP_
