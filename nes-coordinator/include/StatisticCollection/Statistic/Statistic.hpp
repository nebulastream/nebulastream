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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_STATISTIC_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_STATISTIC_HPP_

#include <StatisticCollection/Statistic/StatisticValue.hpp>
namespace NES::Statistic {

/**
 * @brief This class acts as the parent class for all statistics, e.g., synopses or any other statistic
 */
class Statistic {
  public:
    /**
     * @brief Returns the statistic value for this statistic, i.e., a selectivity of 0.5
     * @param characteristic: The characteristic, e.g., Selectivity("f1" > 4)
     * @return
     */
    virtual StatisticValue<> getStatisticValue(const Characteristic& characteristic) const = 0;
};
}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_STATISTIC_HPP_
