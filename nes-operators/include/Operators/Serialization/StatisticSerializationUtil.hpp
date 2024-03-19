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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_SERIALIZATION_STATISTICSERIALIZATIONUTIL_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_SERIALIZATION_STATISTICSERIALIZATIONUTIL_HPP_

#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/TriggerCondition.hpp>
#include <Operators/LogicalOperators/StatisticCollection/WindowStatisticDescriptor.hpp>
#include <StatisticWindowDescriptorMessage.pb.h>

namespace NES {

class SendingPolicyMessage;
class TriggerConditionMessage;
class StatisticWindowDescriptorMessage;

/**
 * @brief Provides functionality to (de-)serialize statistic components, i.e., SendingPolicy or WindowStatisticDescriptor
 */
class StatisticSerializationUtil {
  public:
    /**
     * @brief Serializes the sendingPolicy into the sendingPolicyMessage
     * @param sendingPolicy
     * @param sendingPolicyMessage
     */
    static void serializeSendingPolicy(const Statistic::SendingPolicy& sendingPolicy, SendingPolicyMessage& sendingPolicyMessage);

    /**
     * @brief Serializes the triggerCondition into the triggerConditionMessage
     * @param triggerCondition
     * @param triggerConditionMessage
     */
    static void serializeTriggerCondition(const Statistic::TriggerCondition& triggerCondition, TriggerConditionMessage& triggerConditionMessage);

    /**
     * @brief Serializes the descriptor into the descriptorMessage
     * @param descriptor
     * @param descriptorMessage
     */
    static void serializeDescriptorDetails(const Statistic::WindowStatisticDescriptor& descriptor,
                                           StatisticWindowDescriptorMessage& descriptorMessage);

    /**
     * @brief Creates a SendingPolicy from a sendingPolicyMessage by deserializing it
     * @param sendingPolicyMessage
     * @return SendingPolicyPtr
     */
    static Statistic::SendingPolicyPtr deserializeSendingPolicy(const SendingPolicyMessage& sendingPolicyMessage);

    /**
     * @brief Creates a TriggerCondition from a triggerConditionMessage by deserializing it
     * @param triggerConditionMessage
     * @return TriggerConditionPtr
     */
    static Statistic::TriggerConditionPtr deserializeTriggerCondition(const TriggerConditionMessage& triggerConditionMessage);

    /**
     * @brief Creates a WindowStatisticDescriptor from a descriptorMessage by deserializing it
     * @param descriptorMessage
     * @return WindowStatisticDescriptorPtr
     */
    static Statistic::WindowStatisticDescriptorPtr deserializeDescriptor(const StatisticWindowDescriptorMessage& descriptorMessage);
};

}

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_SERIALIZATION_STATISTICSERIALIZATIONUTIL_HPP_
