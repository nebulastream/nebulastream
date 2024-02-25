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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTIC_SENDINGPOLICY_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTIC_SENDINGPOLICY_HPP_

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
    /**
     * @brief Creates a string representation
     * @return std::string
     */
    [[nodiscard]] virtual std::string toString() const = 0;

    /**
     * @brief Virtual destructor
     */
    virtual ~SendingPolicy() = default;
};
}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_SENDINGPOLICY_HPP_
