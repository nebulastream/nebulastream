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

#ifndef NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_CHARACTERISTIC_DATACHARACTERISTIC_HPP_
#define NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_CHARACTERISTIC_DATACHARACTERISTIC_HPP_
#include <StatisticCollection/Characteristic/Characteristic.hpp>
#include <string>
#include <vector>
namespace NES::Statistic {

/**
 * @brief Represents a data characteristic that results in collecting statistics over a given logical stream and
 * all or a subset of the physical source
 */
class DataCharacteristic : public Characteristic {
  public:
    /**
     * @brief Creates a DataCharacteristic
     * @param type: What type of metric, i.e., selectivity, cardinality, data distribution, ...
     * @param logicalSourceName: Logical source name to collect the statistics
     * @param physicalSourceName: Physical source name to collect the statistics over
     * @return CharacteristicPtr
     */
    static CharacteristicPtr create(MetricPtr type,
                                    const std::string& logicalSourceName,
                                    const std::string& physicalSourceName);

    /**
     * @brief Gets the logical source name
     * @return std::string
     */
    std::string getLogicalSourceName() const;

    /**
     * @brief Gets the physical source name
     * @return std::string
     */
    std::string getPhysicalSourceName() const;

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    bool operator==(const Characteristic& rhs) const override;

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    std::string toString() const override;

    /**
     * @brief Implementing a hash method
     * @return Hash
     */
    size_t hash() const override;

  private:
    /**
     * @brief Creates a DataCharacteristic
     * @param type: What type of metric, i.e., selectivity, cardinality, data distribution, ...
     * @param logicalSourceName: Logical source name to collect the statistics from
     * @param physicalSourceNames: Physical source name to collect the statistics from
     */
    DataCharacteristic(MetricPtr type,
                       std::string logicalSourceName,
                       const std::string& physicalSourceName);

    std::string logicalSourceName;
    std::string physicalSourceName;
};
}// namespace NES::Statistic

#endif // NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_CHARACTERISTIC_DATACHARACTERISTIC_HPP_
