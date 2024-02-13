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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTIC_TRIGGERCONDITION_TRIGGERCONDITION_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTIC_TRIGGERCONDITION_TRIGGERCONDITION_HPP_
#include <StatisticCollection/Statistic/Statistic.hpp>
namespace NES::Statistic {
class TriggerCondition {
  public:
    /**
     * @brief Checks if the corresponding callback should be called
     * @param curStatistic
     * @return True or false
     */
    virtual bool shallTrigger(const Statistic& curStatistic) = 0;
};

/**
 * @brief Checks if the latest statistic is above a threshold
 */
template<typename T>
class ThresholdTrigger : public TriggerCondition {
  private:
    const T threshold;
};

/**
 * @brief Never triggers. Used as a default, if the user does not provide a trigger
 */
class NeverTrigger : public TriggerCondition {
  public:
    bool shallTrigger(const Statistic&) override { return false; }
};
}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_TRIGGERCONDITION_TRIGGERCONDITION_HPP_
