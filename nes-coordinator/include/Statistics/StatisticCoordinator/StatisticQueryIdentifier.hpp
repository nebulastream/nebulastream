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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICS_STATISTICCOORDINATOR_STATISTICQUERYIDENTIFIER_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICS_STATISTICCOORDINATOR_STATISTICQUERYIDENTIFIER_HPP_

#include <memory>
#include <string>

#include <Util/StatisticCollectorType.hpp>

namespace NES::Experimental::Statistics {

/**
 * @brief the class of statistic query identifiers which identify whether a statistic query is already running or not and if so,
 * what the according QueryId is
 */
class StatisticQueryIdentifier {
  public:
    /**
     * @brief the constructor of a statisticQueryIdentifier, which is used to identify statistic queries and their IDs
     * @param logicalSourceName the logicalSource over which a statistic is generated
     * @param fieldName the field on which a statistic is generated
     * @param statisticCollectorType that data structure that is used to generate a statistic
     */
    StatisticQueryIdentifier(const std::string& logicalSourceName,
                             const std::string& fieldName,
                             const StatisticCollectorType statisticCollectorType);

    /**
     * @return the logicalSourceName on which the statistic query runs and over which the statistic is built
     */
    [[nodiscard]] const std::string& getLogicalSourceName() const;

    /**
     * @return the fieldName on which the statistic query runs and over which the statistic is built
     */
    [[nodiscard]] const std::string& getFieldName() const;

    /**
     * @return the data structure that is used to create the statistic
     */
    [[nodiscard]] const StatisticCollectorType& getStatisticCollectorType() const;

    /**
     * @param StatisticQueryIdentifier a object that allows for the identification of
     * tracked statistics via the QueryID
     * @return true if two StatisticQueryIdentifiers are equal
     */
    bool operator==(const StatisticQueryIdentifier& statisticQueryIdentifier) const;

    /**
     * @brief custom hash object for the trackedStatistics unordered_map
     */
    struct Hash {
        std::size_t operator()(const StatisticQueryIdentifier& identifier) const {
            // Combine the hash codes of the components to get a unique identifier
            std::size_t hashValue = 0;
            hashValue ^= std::hash<std::string>()(identifier.logicalSourceName);
            hashValue ^= std::hash<std::string>()(identifier.fieldName);
            hashValue ^= static_cast<std::size_t>(identifier.statisticCollectorType);
            return hashValue;
        }
    };

  private:
    std::string logicalSourceName;
    std::string fieldName;
    StatisticCollectorType statisticCollectorType;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICS_STATISTICCOORDINATOR_STATISTICQUERYIDENTIFIER_HPP_
