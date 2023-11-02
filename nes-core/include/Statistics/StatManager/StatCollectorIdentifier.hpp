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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORIDENTIFIER_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORIDENTIFIER_HPP_

#include <memory>
#include <string>
#include <vector>

#include <Statistics/StatCollectors/StatCollectorType.hpp>

namespace NES::Experimental::Statistics {

/**
 * The class of StatCollectorIdentifiers, that allow to identify a StatCollector (a statistic generating data structure)
 * on the worker
 */
class StatCollectorIdentifier {
  public:
    /**
     * @param logicalSourceName the logical source over which the statistic is generated
     * @param physicalSourceName the physicalSourceName over which the statistic is generated
     * @param fieldName the fieldName over which the statistic is generated
     * @param statCollectorType the Type of statCollector that is used/the statistic that is generated
     */
    StatCollectorIdentifier(const std::string& logicalSourceName,
                            std::vector<std::string>& physicalSourceNames,
                            const std::string& fieldName,
                            const StatCollectorType statCollectorType);

    /**
     * @return returns the logicalSourceName over which the statistic is generated
     */
    [[nodiscard]]std::string& getLogicalSourceName();

    /**
     * @return returns the physicalSourceNames over which the statistic is generated
     */
    std::vector<std::string>& getPhysicalSourceNames();

    /**
     * @return returns the fieldName over which the statistic is generated
     */
    [[nodiscard]]std::string& getFieldName();

    /**
     * @return returns the statCollector type aka the type of statistic
     */
    [[nodiscard]]StatCollectorType& getStatCollectorType();

    /**
     * @brief adds a physicalSourceName to the list/vector of physicalSourceNames that are to be queried
     * @param physicalSourceName the physicalSource that is to be queried
     */
    void setPhysicalSourceName(std::string& physicalSourceName);

    /**
     * @param statCollectorIdentifier a object that allows for the identification of
     * tracked statistics
     * @return true if two StatCollectorIdentifiers are equal
     */
    bool operator==(StatCollectorIdentifier& statCollectorIdentifier);

    /**
     * @brief custom hash object for the trackedStatistics unordered_map
     */
    struct Hash {
        std::size_t operator()(const StatCollectorIdentifier& identifier) const {
            // Combine the hash codes of the components to get a unique identifier
            std::size_t hashValue = 0;
            hashValue ^= std::hash<std::string>()(identifier.logicalSourceName);
            for (const auto& physicalSourceName : identifier.physicalSourceNames) {
                hashValue ^= std::hash<std::string>()(physicalSourceName);
            }
            hashValue ^= std::hash<std::string>()(identifier.fieldName);
            hashValue ^= static_cast<std::size_t>(identifier.statCollectorType);
            return hashValue;
        }
    };

  private:
    std::string logicalSourceName;
    std::vector<std::string> physicalSourceNames;
    std::string fieldName;
    StatCollectorType statCollectorType;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORIDENTIFIER_HPP_
