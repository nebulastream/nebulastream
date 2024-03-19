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

#include <Operators/Serialization/StatisticSerializationUtil.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyASAP.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyAdaptive.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyLazy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/NeverTrigger.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/ThresholdTrigger.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/CountMinDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/HyperLogLogDescriptor.hpp>
#include <Operators/Serialization/ExpressionSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {
void StatisticSerializationUtil::serializeSendingPolicy(const Statistic::SendingPolicy& sendingPolicy,
                                                        SendingPolicyMessage& sendingPolicyMessage) {
    if (sendingPolicy.instanceOf<const Statistic::SendingPolicyAdaptive>()) {
        sendingPolicyMessage.mutable_details()->PackFrom(SendingPolicyMessage_SendingPolicyAdaptive());
    } else if (sendingPolicy.instanceOf<const Statistic::SendingPolicyASAP>()) {
        sendingPolicyMessage.mutable_details()->PackFrom(SendingPolicyMessage_SendingPolicyASAP());
    } else if (sendingPolicy.instanceOf<const Statistic::SendingPolicyLazy>()) {
        sendingPolicyMessage.mutable_details()->PackFrom(SendingPolicyMessage_SendingPolicyLazy());
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

void StatisticSerializationUtil::serializeTriggerCondition(const Statistic::TriggerCondition& triggerCondition,
                                                           TriggerConditionMessage& triggerConditionMessage) {

    if (triggerCondition.instanceOf<const Statistic::NeverTrigger>()) {
        triggerConditionMessage.mutable_details()->PackFrom(TriggerConditionMessage_NeverTrigger());
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

void StatisticSerializationUtil::serializeDescriptorDetails(const Statistic::WindowStatisticDescriptor& descriptor,
                                                            StatisticWindowDescriptorMessage& descriptorMessage) {

    if (descriptor.instanceOf<const Statistic::CountMinDescriptor>()) {
        auto countMinDescriptor = descriptor.as<const Statistic::CountMinDescriptor>();
        StatisticWindowDescriptorMessage_CountMinDetails countMinDetails;
        countMinDetails.set_depth(countMinDescriptor->getDepth());
        descriptorMessage.mutable_details()->PackFrom(countMinDetails);
    } else if (descriptor.instanceOf<const Statistic::CountMinDescriptor>()) {
        descriptorMessage.mutable_details()->PackFrom(StatisticWindowDescriptorMessage_HyperLogLogDetails());
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

Statistic::SendingPolicyPtr
StatisticSerializationUtil::deserializeSendingPolicy(const SendingPolicyMessage& sendingPolicyMessage) {
    if (sendingPolicyMessage.details().Is<SendingPolicyMessage_SendingPolicyAdaptive>()) {
        return Statistic::SendingPolicyAdaptive::create();
    } else if (sendingPolicyMessage.details().Is<SendingPolicyMessage_SendingPolicyLazy>()) {
        return Statistic::SendingPolicyLazy::create();
    } else if (sendingPolicyMessage.details().Is<SendingPolicyMessage_SendingPolicyASAP>()) {
        return Statistic::SendingPolicyASAP::create();
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

Statistic::TriggerConditionPtr
StatisticSerializationUtil::deserializeTriggerCondition(const TriggerConditionMessage& triggerConditionMessage) {
    if (triggerConditionMessage.details().Is<TriggerConditionMessage_NeverTrigger>()) {
        return Statistic::NeverTrigger::create();
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

Statistic::WindowStatisticDescriptorPtr
StatisticSerializationUtil::deserializeDescriptor(const StatisticWindowDescriptorMessage& descriptorMessage) {

    Statistic::WindowStatisticDescriptorPtr statisticDescriptor;

    // 1. Deserializing the field expression
    auto expression = ExpressionSerializationUtil::deserializeExpression(descriptorMessage.field());
    if (!expression->instanceOf<FieldAccessExpressionNode>()) {
        NES_THROW_RUNTIME_ERROR("Expects a FieldAccessExpressionNode for deserializing a WindowStatisticDescriptor!");
    }


    // 2. Deserializing the descriptor details
    if (descriptorMessage.details().Is<StatisticWindowDescriptorMessage_CountMinDetails>()) {
        StatisticWindowDescriptorMessage_CountMinDetails countMinDetails;
        descriptorMessage.details().UnpackTo(&countMinDetails);
        statisticDescriptor = Statistic::CountMinDescriptor::create(expression->as<FieldAccessExpressionNode>(), descriptorMessage.width(), countMinDetails.depth());

    } else if (descriptorMessage.details().Is<StatisticWindowDescriptorMessage_HyperLogLogDetails>()) {
        StatisticWindowDescriptorMessage_HyperLogLogDetails hyperLogLogDetails;
        descriptorMessage.details().UnpackTo(&hyperLogLogDetails);
        statisticDescriptor = Statistic::HyperLogLogDescriptor::create(expression->as<FieldAccessExpressionNode>(), descriptorMessage.width());
    } else {
        NES_NOT_IMPLEMENTED();
    }

    // 3. Deserializing sending policy and trigger condition
    auto sendingPolicy = StatisticSerializationUtil::deserializeSendingPolicy(descriptorMessage.sendingpolicy());
    auto triggerCondition = StatisticSerializationUtil::deserializeTriggerCondition(descriptorMessage.triggercondition());
    statisticDescriptor->setSendingPolicy(sendingPolicy);
    statisticDescriptor->setTriggerCondition(triggerCondition);

    return statisticDescriptor;
}

}// namespace NES