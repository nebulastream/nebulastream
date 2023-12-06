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

#include <Operators/LogicalOperators/Statistics/StatisticCollectorType.hpp>
#include <Identifiers.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES::Experimental::Statistics {

/**
 * The class of StatisticCollectorIdentifiers, that allow to identify a StatisticCollector (a statistic generating data structure)
 * on the worker
 */
class StatisticCollectorIdentifier {
  public:
    /**
     * @param logicalSourceName the logical source over which the statistic is generated
     * @param physicalSourceName the physicalSource over which the statistic is generated
     * @param nodeId the id of the node on which the statistic was generated
     * @param fieldName the fieldName over which the statistic is generated
     * @param statisticCollectorType the Type of statisticCollector that is used/the statistic that is generated
     */
    StatisticCollectorIdentifier(const std::string& logicalSourceName,
                                 const std::string& physicalSourceName,
                                 const TopologyNodeId& nodeId,
                                 const std::string& fieldName,
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
     * @return returns the node on which the statistic was created
     */
    [[nodiscard]] const TopologyNodeId& getNodeId() const;

    /**
     * @return returns the fieldName over which the statistic is generated
     */
    [[nodiscard]] const std::string& getFieldName() const;

    /**
     * @return returns the StatisticCollector type aka the type of statistic
     */
    [[nodiscard]] const StatisticCollectorType& getStatisticCollectorType() const;

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
            hashValue ^= std::hash<uint64_t>()(identifier.nodeId);
            hashValue ^= std::hash<std::string>()(identifier.fieldName);
            hashValue ^= static_cast<std::size_t>(identifier.statisticCollectorType);
            return hashValue;
        }
    };

  private:
    std::string logicalSourceName;
    std::string physicalSourceName;
    TopologyNodeId nodeId;
    std::string fieldName;
    StatisticCollectorType statisticCollectorType;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_WORKER_INCLUDE_STATISTICS_STATISTICMANAGER_STATISTICCOLLECTORIDENTIFIER_HPP_
