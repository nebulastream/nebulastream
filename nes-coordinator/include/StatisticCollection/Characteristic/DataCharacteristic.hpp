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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTIC_CHARACTERISTIC_DATACHARACTERISTIC_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTIC_CHARACTERISTIC_DATACHARACTERISTIC_HPP_
#include <StatisticCollection/Characteristic/Characteristic.hpp>
#include <string>
#include <vector>
namespace NES::Statistic {

/**
 * @brief Represents a data characteristic that results in collecting statistics over a given logical stream and
 * all or a subset of the physical streams
 */
class DataCharacteristic : public Characteristic {
  public:
    /**
     * @brief Creates a DataCharacteristic
     * @param type: What type of metric, i.e., selectivity, cardinality, data distribution, ...
     * @param logicalStreamName: Logical stream name to collect the statistics
     * @param physicalStreamNames: If set to {}, then all physical streams are being selected
     */
    DataCharacteristic(Metric type,
                       const std::string& logicalStreamName,
                       const std::initializer_list<std::string>& physicalStreamNames = {})
        : Characteristic(type), logicalStreamName(logicalStreamName), physicalStreamNames(physicalStreamNames) {}

    /**
     * @brief Gets the logical stream name
     * @return std::string
     */
    std::string getLogicalStreamName() const { return logicalStreamName; }

    /**
     * @brief Gets the physical stream names
     * @return std::vector<std::string>
     */
    std::vector<std::string> getPhysicalStreamNames() const { return physicalStreamNames; }

  private:
    std::string logicalStreamName;
    std::vector<std::string> physicalStreamNames;
};
}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_CHARACTERISTIC_DATACHARACTERISTIC_HPP_
