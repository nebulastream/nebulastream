/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORYDETAILS_HPP_
#define NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORYDETAILS_HPP_
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>
#include <Windowing/WindowActions/BaseWindowActionDescriptor.hpp>
#include <Windowing/WindowActions/ExecutableCompleteAggregationTriggerAction.hpp>
#include <Windowing/WindowActions/ExecutableSliceAggregationTriggerAction.hpp>
#include <Windowing/WindowAggregations/ExecutableAVGAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableCountAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableMaxAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableMinAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowHandler/AggregationWindowHandler.hpp>
#include <Windowing/WindowHandler/JoinHandler.hpp>

namespace NES::Windowing {

class WindowHandlerFactoryDetails {
  public:
    /**
    * @brief Factory to create a typed window handler.
    * @tparam KeyType type of the key field
    * @tparam InputType type of the input aggregation field
    * @tparam PartialAggregateType type of the partial aggregation field
    * @tparam FinalAggregateType type of the final aggregation field
    * @param windowDefinition window definition
    * @param windowAggregation executable window aggregation
    * @return AbstractWindowHandlerPtr
    */
    template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
    static AbstractWindowHandlerPtr createAggregationWindow(LogicalWindowDefinitionPtr windowDefinition,
                                                            std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> executableWindowAggregation) {
        auto policy = windowDefinition->getTriggerPolicy();
        BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger;
        if (policy->getPolicyType() == triggerOnTime) {
            OnTimeTriggerDescriptionPtr triggerDesc = std::dynamic_pointer_cast<OnTimeTriggerPolicyDescription>(policy);
            executablePolicyTrigger = ExecutableOnTimeTriggerPolicy::create(triggerDesc->getTriggerTimeInMs());
        } else {
            NES_FATAL_ERROR("Aggregation Handler: mode=" << policy->getPolicyType() << " not implemented");
        }

        auto action = windowDefinition->getTriggerAction();
        BaseExecutableWindowActionPtr<KeyType, InputType, PartialAggregateType, FinalAggregateType> executableWindowAction;
        if (action->getActionType() == WindowAggregationTriggerAction) {
            executableWindowAction = ExecutableCompleteAggregationTriggerAction<KeyType, InputType, PartialAggregateType, FinalAggregateType>::create(windowDefinition, executableWindowAggregation);
        } else if (action->getActionType() == SliceAggregationTriggerAction) {
            executableWindowAction = ExecutableSliceAggregationTriggerAction<KeyType, InputType, PartialAggregateType, FinalAggregateType>::create(windowDefinition, executableWindowAggregation);
        } else {
            NES_FATAL_ERROR("Aggregation Handler: mode=" << action->getActionType() << " not implemented");
        }

        //add compile method return handler
        //create the action
        return std::make_shared<AggregationWindowHandler<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(windowDefinition, executableWindowAggregation, executablePolicyTrigger, executableWindowAction);
    }

    template<class KeyType, class InputType>
    static AbstractWindowHandlerPtr createWindowHandlerForAggregationForKeyAndInput(LogicalWindowDefinitionPtr windowDefinition) {
        auto onField = windowDefinition->getWindowAggregation()->on();
        auto asField = windowDefinition->getWindowAggregation()->as();
        switch (windowDefinition->getWindowAggregation()->getType()) {
            case WindowAggregationDescriptor::Avg:
                return createAggregationWindow<KeyType, InputType, AVGPartialType<InputType>, AVGResultType>(windowDefinition, ExecutableAVGAggregation<InputType>::create());
            case WindowAggregationDescriptor::Count:
                return createAggregationWindow<KeyType, InputType, CountType, CountType>(windowDefinition, ExecutableCountAggregation<InputType>::create());
            case WindowAggregationDescriptor::Max:
                return createAggregationWindow<KeyType, InputType, InputType, InputType>(windowDefinition, ExecutableMaxAggregation<InputType>::create());
            case WindowAggregationDescriptor::Min:
                return createAggregationWindow<KeyType, InputType, InputType, InputType>(windowDefinition, ExecutableMinAggregation<InputType>::create());
            case WindowAggregationDescriptor::Sum:
                return createAggregationWindow<KeyType, InputType, InputType, InputType>(windowDefinition, ExecutableSumAggregation<InputType>::create());
        }
        NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: Avg aggregation currently not supported");
    };

    template<typename KeyType>
    static AbstractWindowHandlerPtr createWindowHandlerForAggregationKeyType(LogicalWindowDefinitionPtr windowDefinition) {
        auto logicalAggregationInput = windowDefinition->getWindowAggregation()->on();
        auto physicalInputType = DefaultPhysicalTypeFactory().getPhysicalType(logicalAggregationInput->getStamp());
        if (physicalInputType->isBasicType()) {
            auto basicInputType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalInputType);
            switch (basicInputType->getNativeType()) {
                case BasicPhysicalType::UINT_8: return createWindowHandlerForAggregationForKeyAndInput<KeyType, uint8_t>(windowDefinition);
                case BasicPhysicalType::UINT_16: return createWindowHandlerForAggregationForKeyAndInput<KeyType, uint16_t>(windowDefinition);
                case BasicPhysicalType::UINT_32: return createWindowHandlerForAggregationForKeyAndInput<KeyType, uint32_t>(windowDefinition);
                case BasicPhysicalType::UINT_64: return createWindowHandlerForAggregationForKeyAndInput<KeyType, uint64_t>(windowDefinition);
                case BasicPhysicalType::INT_8: return createWindowHandlerForAggregationForKeyAndInput<KeyType, int8_t>(windowDefinition);
                case BasicPhysicalType::INT_16: return createWindowHandlerForAggregationForKeyAndInput<KeyType, int16_t>(windowDefinition);
                case BasicPhysicalType::INT_32: return createWindowHandlerForAggregationForKeyAndInput<KeyType, int32_t>(windowDefinition);
                case BasicPhysicalType::INT_64: return createWindowHandlerForAggregationForKeyAndInput<KeyType, int64_t>(windowDefinition);
                case BasicPhysicalType::FLOAT: return createWindowHandlerForAggregationForKeyAndInput<KeyType, float>(windowDefinition);
                case BasicPhysicalType::DOUBLE: return createWindowHandlerForAggregationForKeyAndInput<KeyType, double>(windowDefinition);
                case BasicPhysicalType::CHAR:
                case BasicPhysicalType::BOOLEAN: NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: we dont support aggregation of Chars or Booleans");
            };
        }
        NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: currently we dont support non basic input types");
    };
};
}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORYDETAILS_HPP_
