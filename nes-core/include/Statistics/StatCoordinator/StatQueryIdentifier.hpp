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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATCOORDINATOR_STATQUERYIDENTIFIER_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATCOORDINATOR_STATQUERYIDENTIFIER_HPP_

#include <memory>

#include <Statistics/StatCollectors/StatCollectorType.hpp>

namespace NES {

namespace Experimental::Statistics {

class StatQueryIdentifier {
  public:
    /**
     *
     * @param physicalSourceName
     * @param fieldName
     * @param statCollectorType
     */
    StatQueryIdentifier(const std::string& physicalSourceName,
                        const std::string& fieldName,
                        const StatCollectorType statCollectorType);

    /**
     *
     * @return
     */
    std::string getLogicalSourceName() const;

    /**
     *
     * @return
     */
    std::string getFieldName() const;

    /**
     *
     * @return
     */
    StatCollectorType getStatCollectorType() const;

    /**
     * @param StatQueryIdentifier a object that allows for the identification of
     * tracked statistics via the QueryID
     * @return true if two StatQueryIdentifier are equal
     */
    bool operator==(const StatQueryIdentifier& statQueryIdentifier) const;

    /**
     * @brief custom hash object for the trackedStatistics unordered_map
     */
    struct Hash {
        std::size_t operator()(const StatQueryIdentifier& identifier) const {
            // Combine the hash codes of the components to get a unique identifier
            std::size_t hashValue = 0;
            hashValue ^= std::hash<std::string>()(identifier.getLogicalSourceName());
            hashValue ^= std::hash<std::string>()(identifier.getFieldName());
            hashValue ^= static_cast<std::size_t>(identifier.getStatCollectorType());
            return hashValue;
        }
    };

  private:
    std::string logicalSourceName;
    std::string fieldName;
    StatCollectorType statCollectorType;
};
}// namespace Experimental::Statistics
}// namespace NES

#endif//NES_NES_CORE_INCLUDE_STATISTICS_STATCOORDINATOR_STATQUERYIDENTIFIER_HPP_
