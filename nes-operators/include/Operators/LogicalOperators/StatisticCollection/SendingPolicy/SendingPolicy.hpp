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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_SENDINGPOLICY_SENDINGPOLICY_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_SENDINGPOLICY_SENDINGPOLICY_HPP_

#include <StatisticIdentifiers.hpp>
#include <string>
#include <memory>

namespace NES::Statistic {

class SendingPolicy;
using SendingPolicyPtr = std::shared_ptr<SendingPolicy>;

/**
 * @brief This class acts as an abstract class for all possible SendingPolicies
 */
class SendingPolicy {
  public:
    explicit SendingPolicy(StatisticDataCodec sinkDataCodec) : sinkDataCodec(sinkDataCodec) {}

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    virtual bool operator==(const SendingPolicy& rhs) const = 0;

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if NOT equal otherwise false
     */
    virtual bool operator!=(const SendingPolicy& rhs) const {return !(*this == rhs); };

    /**
     * @brief Returns the data type of the statistic sink.
     * @return Statistic::StatisticDataCodec
     */
    virtual StatisticDataCodec getSinkDataCodec() const { return sinkDataCodec; };

    /**
     * @brief Checks if the current SendingPolicy is of type SendingPolicyType
     * @tparam SendingPolicyType
     * @return bool true if node is of SendingPolicyType
     */
    template<class SendingPolicyType>
    bool instanceOf() const {
        if (dynamic_cast<SendingPolicyType*>(this)) {
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
     * @brief Virtual destructor
     */
    virtual ~SendingPolicy() = default;

  protected:
    StatisticDataCodec sinkDataCodec;
};
}// namespace NES::Statistic

#endif // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_SENDINGPOLICY_SENDINGPOLICY_HPP_
