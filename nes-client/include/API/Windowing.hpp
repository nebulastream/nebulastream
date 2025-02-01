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

#pragma once

#include <memory>
#include <API/TimeUnit.hpp>

namespace NES
{

class NodeFunction;
using NodeFunctionPtr = std::shared_ptr<NodeFunction>;

class FunctionItem;

namespace Windowing
{

class WindowType;
using WindowTypePtr = std::shared_ptr<WindowType>;

class WindowAggregationDescriptor;
using WindowAggregationDescriptorPtr = std::shared_ptr<WindowAggregationDescriptor>;

class TimeMeasure;
class TimeCharacteristic;
using TimeCharacteristicPtr = std::shared_ptr<TimeCharacteristic>;

class WatermarkStrategyDescriptor;
using WatermarkStrategyDescriptorPtr = std::shared_ptr<WatermarkStrategyDescriptor>;
}
}
namespace NES::API
{

class WindowAggregation;
using WindowAggregationPtr = std::shared_ptr<WindowAggregation>;
class WindowAggregation
{
public:
    WindowAggregation(Windowing::WindowAggregationDescriptorPtr windowAggregationDescriptor);
    API::WindowAggregationPtr as(const FunctionItem& asField);
    const Windowing::WindowAggregationDescriptorPtr aggregation;
};

API::WindowAggregationPtr Sum(const FunctionItem& onField);
API::WindowAggregationPtr Min(const FunctionItem& onField);
API::WindowAggregationPtr Max(const FunctionItem& onField);
API::WindowAggregationPtr Count(const FunctionItem& onField);
API::WindowAggregationPtr Median(const FunctionItem& onField);
API::WindowAggregationPtr Avg(const FunctionItem& onField);

API::WindowAggregationPtr SamplingWoR(const FunctionItem& onField);
Windowing::TimeCharacteristicPtr EventTime(const FunctionItem& onField);
Windowing::TimeCharacteristicPtr EventTime(const FunctionItem& onField, const Windowing::TimeUnit& unit);
Windowing::TimeCharacteristicPtr IngestionTime();
Windowing::TimeMeasure Milliseconds(uint64_t milliseconds);
Windowing::TimeMeasure Seconds(uint64_t seconds);
Windowing::TimeMeasure Minutes(uint64_t minutes);
Windowing::TimeMeasure Hours(uint64_t hours);
Windowing::TimeMeasure Days(uint64_t days);
Windowing::TimeUnit Milliseconds();
Windowing::TimeUnit Seconds();
Windowing::TimeUnit Minutes();
Windowing::TimeUnit Hours();
Windowing::TimeUnit Days();
[[maybe_unused]] NodeFunctionPtr RecordCreationTs();

}
