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

#ifndef NES_NES_WORKER_INCLUDE_STATISTICS_STATISTICMANAGER_STATISTICCOLLECTORIDENTIFIER_HPP_
#define NES_NES_WORKER_INCLUDE_STATISTICS_STATISTICMANAGER_STATISTICCOLLECTORIDENTIFIER_HPP_

#include <Identifiers.hpp>
#include <Util/StatisticCollectorType.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES::Experimental::Statistics {

/**
 * The class of StatisticCollectorIdentifiers, that allow to identify a StatisticCollectorFormat (a statistic generating data structure)
 * on the worker
 */
class StatisticCollectorIdentifier {
  public:
    /**
     * @param logicalSourceName the logical source over which the statistic is generated
     * @param physicalSourceName the physicalSource over which the statistic is generated
     * @param fieldName the fieldName over which the statistic is generated
     * @param startTime the start time of the time period over which the statistic was constructed
     * @param endTime the end time of the time period over which the statistic was constructed
     * @param statisticCollectorType the Type of statisticCollector that is used/the statistic that is generated
     */
    StatisticCollectorIdentifier(const std::string& logicalSourceName,
                                 const std::string& physicalSourceName,
                                 const std::string& fieldName,
                                 const time_t startTime,
                                 const time_t endTime,
                                 const StatisticCollectorType statisticCollectorType);

    /**
     * @return returns the logicalSourceName over which the statistic is generated
     */
    [[nodiscard]] const std::string& getLogicalSourceName() const;

    /**
     * @return returns the physicalSourceName over which the statistic is generated
     */
    [[nodiscard]] const std::string& getPhysicalSourceName() const;

    /**
     * @return returns the fieldName over which the statistic is generated
     */
    [[nodiscard]] const std::string& getFieldName() const;

    /**
     * @return returns the StatisticCollectorFormat type aka the type of statistic
     */
    [[nodiscard]] const StatisticCollectorType& getStatisticCollectorType() const;

    /**
     * @return returns the startTime of the time period over which the statistic was constructed
     */
    [[nodiscard]] time_t getStartTime() const;

    /**
     * @return returns the endTime of the time period over which the statistic was constructed
     */
    [[nodiscard]] time_t getEndTime() const;

    /**
     * @param statisticCollectorType a object that allows for the identification of
     * tracked statistics
     * @return true if two StatisticCollectorIdentifiers are equal
     */
    bool operator==(const StatisticCollectorIdentifier& statisticCollectorIdentifier) const;

    /**
     * @brief custom hash object for the trackedStatistics unordered_map
     */
    struct Hash {
        std::size_t operator()(const StatisticCollectorIdentifier& identifier) const {
            // Combine the hash codes of the components to get a unique identifier
            std::size_t hashValue = 0;
            hashValue ^= std::hash<std::string>()(identifier.logicalSourceName);
            hashValue ^= std::hash<std::string>()(identifier.physicalSourceName);
            hashValue ^= std::hash<std::string>()(identifier.fieldName);
            hashValue ^= std::hash<time_t>()(identifier.startTime);
            hashValue ^= std::hash<time_t>()(identifier.endTime);
            hashValue ^= static_cast<std::size_t>(identifier.statisticCollectorType);
            return hashValue;
        }
    };

  private:
    std::string logicalSourceName;
    std::string physicalSourceName;
    std::string fieldName;
    time_t startTime;
    time_t endTime;
    StatisticCollectorType statisticCollectorType;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_WORKER_INCLUDE_STATISTICS_STATISTICMANAGER_STATISTICCOLLECTORIDENTIFIER_HPP_
