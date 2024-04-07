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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_STATISTICVALUE_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_STATISTICVALUE_HPP_
#include <vector>
namespace NES::Statistic {

/**
 * @brief This class acts as an abstract class for all possible statistic values, e.g., a specific cardinality of 25
 */
template<typename StatType = double>
class StatisticValue {
  public:

    /**
     * @brief Constructor
     * @param value
     */
    explicit StatisticValue(StatType value) : value(value) {}

    /**
     * @brief Getter for the underlying value
     * @return
     */
    virtual StatType getValue() const { return value; }

    /**
     * @brief Virtual default destructor
     */
    virtual ~StatisticValue() = default;

  private:
    StatType value;
};

/**
 * @brief This class acts as a container for multiple probe items
 */
template<typename StatType = double>
class ProbeResult {
  public:
    void addStatisticValue(StatisticValue<StatType> statisticValue) {
        probeItems.emplace_back(statisticValue);
    }

    /**
     * @brief Returns a const reference to the probeItems
     * @return const std::vector<StatisticValue<StatType>>&
     */
    const std::vector<StatisticValue<StatType>>& getProbeItems() const {
        return probeItems;
    }

  private:
    std::vector<StatisticValue<StatType>> probeItems;
};

}// namespace NES::Statistic
#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_STATISTICVALUE_HPP_
