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


namespace NES::Statistic {
/**
 * @brief This class acts as an abstract class for all possible SendingPolicies
 */
class SendingPolicy {};

// defines for the sending policies. This way, we reduce the number of ()
#define SENDING_ASAP SendingPolicyASAP()
#define SENDING_LAZY SendingPolicyLazy()
#define SENDING_ADAPTIVE SendingPolicyAdaptive()

/**
 * @brief Represents a sending policy, where a created statistic is send ASAP to the store
 */
class SendingPolicyASAP : public SendingPolicy {};

/**
 * @brief Represents a sending policy, where a created statistic is send to the store, if it gets probed
 */
class SendingPolicyLazy : public SendingPolicy {};

/**
 * @brief Represents a sending policy, where a created statistic is send to the store, depending on external factors
 */
class SendingPolicyAdaptive : public SendingPolicy {};

} // namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_SENDINGPOLICY_HPP_
