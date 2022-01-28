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

#ifndef NES_INCLUDE_WINDOWING_WINDOWINGFORWARDREFS_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWINGFORWARDREFS_HPP_

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

namespace NES {

class AttributeField;
using AttributeFieldPtr = std::shared_ptr<AttributeField>;

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

class FieldAccessExpressionNode;
using FieldAccessExpressionNodePtr = std::shared_ptr<FieldAccessExpressionNode>;

class ExpressionItem;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

}// namespace NES

namespace NES::Windowing {

class WindowOperatorHandler;
using WindowOperatorHandlerPtr = std::shared_ptr<WindowOperatorHandler>;

class BaseExecutableWindowTriggerPolicy;
using BaseExecutableWindowTriggerPolicyPtr = std::shared_ptr<BaseExecutableWindowTriggerPolicy>;

class ExecutableOnTimeTriggerPolicy;
using ExecutableOnTimeTriggerPtr = std::shared_ptr<ExecutableOnTimeTriggerPolicy>;

class ExecutableOnWatermarkChangeTriggerPolicy;
using ExecutableOnWatermarkChangeTriggerPolicyPtr = std::shared_ptr<ExecutableOnWatermarkChangeTriggerPolicy>;

class BaseWindowTriggerPolicyDescriptor;
using WindowTriggerPolicyPtr = std::shared_ptr<BaseWindowTriggerPolicyDescriptor>;

class BaseWindowActionDescriptor;
using WindowActionDescriptorPtr = std::shared_ptr<BaseWindowActionDescriptor>;

class AbstractWindowHandler;
using AbstractWindowHandlerPtr = std::shared_ptr<AbstractWindowHandler>;

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class BaseExecutableWindowAction;
template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
using BaseExecutableWindowActionPtr =
    std::shared_ptr<BaseExecutableWindowAction<KeyType, InputType, PartialAggregateType, FinalAggregateType>>;

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class ExecutableCompleteAggregationTriggerAction;
template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
using ExecutableCompleteAggregationTriggerActionPtr =
    std::shared_ptr<ExecutableCompleteAggregationTriggerAction<KeyType, InputType, PartialAggregateType, FinalAggregateType>>;

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class ExecutableSliceAggregationTriggerAction;
template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
using ExecutableSliceAggregationTriggerActionPtr =
    std::shared_ptr<ExecutableSliceAggregationTriggerAction<KeyType, InputType, PartialAggregateType, FinalAggregateType>>;

class LogicalWindowDefinition;
using LogicalWindowDefinitionPtr = std::shared_ptr<LogicalWindowDefinition>;

class WindowAggregationDescriptor;
using WindowAggregationPtr = std::shared_ptr<WindowAggregationDescriptor>;

template<typename InputType, typename PartialAggregateType, typename FinalAggregateName>
class ExecutableWindowAggregation;
//typedef std::shared_ptr<ExecutableWindowAggregation> ExecutableWindowAggregationPtr;

class WindowManager;
using WindowManagerPtr = std::shared_ptr<WindowManager>;

template<class PartialAggregateType>
class WindowSliceStore;

template<class ValueType>
class WindowedJoinSliceListStore;

class SliceMetaData;

class WindowType;
using WindowTypePtr = std::shared_ptr<WindowType>;

class TumblingWindow;
using TumblingWindowPtr = std::shared_ptr<TumblingWindow>;

class SlidingWindow;
using SlidingWindowPtr = std::shared_ptr<SlidingWindow>;

class TimeMeasure;

class TimeCharacteristic;
using TimeCharacteristicPtr = std::shared_ptr<TimeCharacteristic>;

class DistributionCharacteristic;
using DistributionCharacteristicPtr = std::shared_ptr<DistributionCharacteristic>;

inline uint64_t getTsFromClock() { return time(nullptr) * 1000; }

class WindowAggregationDescriptor;
using WindowAggregationDescriptorPtr = std::shared_ptr<WindowAggregationDescriptor>;

class WindowState;

class WatermarkStrategy;
using WatermarkStrategyPtr = std::shared_ptr<WatermarkStrategy>;

class EventTimeWatermarkStrategy;
using EventTimeWatermarkStrategyPtr = std::shared_ptr<EventTimeWatermarkStrategy>;

class WatermarkStrategyDescriptor;
using WatermarkStrategyDescriptorPtr = std::shared_ptr<WatermarkStrategyDescriptor>;

}// namespace NES::Windowing

#endif  // NES_INCLUDE_WINDOWING_WINDOWINGFORWARDREFS_HPP_
