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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_TRIGGERCONDITION_NEVERTRIGGER_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_TRIGGERCONDITION_NEVERTRIGGER_HPP_

#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/TriggerCondition.hpp>

namespace NES::Statistic {

/**
 * @brief Never triggers. Used as a default, if the user does not provide a trigger
 */
class NeverTrigger : public TriggerCondition {
  public:
    /**
     * @brief Creates a NeverTrigger wrapped in a TriggerConditionPtr
     * @return TriggerConditionPtr
     */
    static TriggerConditionPtr create();

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    bool operator==(const TriggerCondition& rhs) const override;

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    std::string toString() const override;

    /**
     * @brief Virtual deconstructor
     */
    ~NeverTrigger() override = default;

    /**
     * @brief Never returns true
     * @return Always false
     */
    bool shallTrigger(const ExpressionNode&) override;
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_TRIGGERCONDITION_NEVERTRIGGER_HPP_
