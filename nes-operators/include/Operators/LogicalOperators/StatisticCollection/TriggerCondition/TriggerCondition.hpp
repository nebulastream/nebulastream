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
#include <Operators/LogicalOperators/StatisticCollection/Statistic/Statistic.hpp>
namespace NES::Statistic {

class TriggerCondition;
using TriggerConditionPtr = std::shared_ptr<TriggerCondition>;
class TriggerCondition {
  public:
    /**
     * @brief Checks if the corresponding callback should be called
     * @param curStatistic
     * @return True or false
     */
    virtual bool shallTrigger(const Statistic& curStatistic) = 0;

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    virtual bool operator==(const TriggerCondition& rhs) const = 0;

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if NOT equal otherwise false
     */
    virtual bool operator!=(const TriggerCondition& rhs) const = 0;

    /**
     * @brief Checks if the current TriggerCondition is of type TriggerConditionType
     * @tparam TriggerCondition
     * @return bool true if node is of TriggerCondition
     */
    template<class TriggerCondition>
    bool instanceOf() {
        if (dynamic_cast<TriggerCondition*>(this)) {
            return true;
        }
        return false;
    };

    /**
     * @brief Checks if the current TriggerCondition is of type const TriggerConditionType
     * @tparam TriggerCondition
     * @return bool true if node is of TriggerCondition
     */
    template<class TriggerCondition>
    bool instanceOf() const {
        if (dynamic_cast<const TriggerCondition*>(this)) {
            return true;
        }
        return false;
    };

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    [[nodiscard]] virtual std::string toString() const = 0;

    /**
     * @brief Virtual deconstructor
     */
    virtual ~TriggerCondition() = default;
};
}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_TRIGGERCONDITION_TRIGGERCONDITION_HPP_
