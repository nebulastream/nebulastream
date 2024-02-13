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

namespace NES::Statistic {

/**
 * @brief This class acts as an abstract class for all possible statistic values
 */
template<typename StatType = double>
class StatisticValue {
  public:
    /**
     * @brief Getter for the underlying value
     * @return
     */
    virtual StatType getValue() const { return value; }

    /**
     * @brief Virtual default deconstructor
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
  private:
    std::vector<StatisticValue<StatType>> probeItems;
};

} // namespace NES::Statistic
#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_STATISTICVALUE_HPP_
