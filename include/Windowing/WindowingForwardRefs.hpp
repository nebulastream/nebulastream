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

#ifndef NES_INCLUDE_WINDOWING_WINDOWINGFORWARDREFS_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWINGFORWARDREFS_HPP_

#include "memory"

namespace NES {

class TupleBuffer;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

class FieldAccessExpressionNode;
typedef std::shared_ptr<FieldAccessExpressionNode> FieldAccessExpressionNodePtr;

class ExpressionItem;

class QueryManager;
typedef std::shared_ptr<QueryManager> QueryManagerPtr;

class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class MemoryLayout;
typedef std::shared_ptr<MemoryLayout> MemoryLayoutPtr;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

}// namespace NES

namespace NES::Windowing {

class ExecutableOnTimeTriggerPolicy;
typedef std::shared_ptr<ExecutableOnTimeTriggerPolicy> ExecutableOnTimeTriggerPtr;

class BaseWindowTriggerPolicyDescriptor;
typedef std::shared_ptr<BaseWindowTriggerPolicyDescriptor> WindowTriggerPolicyPtr;

class BaseWindowActionDescriptor;
typedef std::shared_ptr<BaseWindowActionDescriptor> WindowActionDescriptorPtr;

class AbstractWindowHandler;
typedef std::shared_ptr<AbstractWindowHandler> AbstractWindowHandlerPtr;

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class BaseExecutableWindowAction;
template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
using BaseExecutableWindowActionPtr = std::shared_ptr<BaseExecutableWindowAction<KeyType, InputType, PartialAggregateType, FinalAggregateType>>;

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class ExecutableCompleteAggregationTriggerAction;
template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
using ExecutableCompleteAggregationTriggerActionPtr = std::shared_ptr<ExecutableCompleteAggregationTriggerAction<KeyType, InputType, PartialAggregateType, FinalAggregateType>>;

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class ExecutableSliceAggregationTriggerAction;
template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
using ExecutableSliceAggregationTriggerActionPtr = std::shared_ptr<ExecutableSliceAggregationTriggerAction<KeyType, InputType, PartialAggregateType, FinalAggregateType>>;

class LogicalWindowDefinition;
typedef std::shared_ptr<LogicalWindowDefinition> LogicalWindowDefinitionPtr;

class WindowAggregationDescriptor;
typedef std::shared_ptr<WindowAggregationDescriptor> WindowAggregationPtr;

template<typename InputType, typename PartialAggregateType, typename FinalAggregateName>
class ExecutableWindowAggregation;
//typedef std::shared_ptr<ExecutableWindowAggregation> ExecutableWindowAggregationPtr;

class WindowManager;
typedef std::shared_ptr<WindowManager> WindowManagerPtr;

template<class PartialAggregateType>
class WindowSliceStore;

class SliceMetaData;

class WindowType;
typedef std::shared_ptr<WindowType> WindowTypePtr;

class TumblingWindow;
typedef std::shared_ptr<TumblingWindow> TumblingWindowPtr;

class SlidingWindow;
typedef std::shared_ptr<SlidingWindow> SlidingWindowPtr;

class TimeMeasure;

class TimeCharacteristic;
typedef std::shared_ptr<TimeCharacteristic> TimeCharacteristicPtr;

class DistributionCharacteristic;
typedef std::shared_ptr<DistributionCharacteristic> DistributionCharacteristicPtr;

inline uint64_t getTsFromClock() {
    return time(NULL) * 1000;
}

class WindowAggregationDescriptor;
typedef std::shared_ptr<WindowAggregationDescriptor> WindowAggregationDescriptorPtr;

class WindowState;

class WatermarkStrategy;
typedef std::shared_ptr<WatermarkStrategy> WatermarkStrategyPtr;

class EventTimeWatermarkStrategy;
typedef std::shared_ptr<EventTimeWatermarkStrategy> EventTimeWatermarkStrategyPtr;

class WatermarkStrategyDescriptor;
typedef std::shared_ptr<WatermarkStrategyDescriptor> WatermarkStrategyDescriptorPtr;

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWINGFORWARDREFS_HPP_
