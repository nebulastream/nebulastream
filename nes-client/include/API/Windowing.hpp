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
/**
 * @brief The following declares API functions for windowing.
 */
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

/**
 * @brief Defines a Sum Aggregation function on a particular field.
 * @param FunctionItem Attribute("field-name") the field which should be aggregated.
 * @return A descriptor of the aggregation function.
 */
API::WindowAggregationPtr Sum(const FunctionItem& onField);

/**
 * @brief Defines a Min Aggregation function on a particular field.
 * @param FunctionItem Attribute("field-name") the field which should be aggregated.
 * @return A descriptor of the aggregation function.
 */
API::WindowAggregationPtr Min(const FunctionItem& onField);

/**
 * @brief Defines a Max Aggregation function on a particular field.
 * @param FunctionItem Attribute("field-name") the field which should be aggregated.
 * @return A descriptor of the aggregation function.
 */
API::WindowAggregationPtr Max(const FunctionItem& onField);

/**
 * @brief Defines a Count Aggregation function on a particular field.
 * @return A descriptor of the aggregation function.
 */
API::WindowAggregationPtr Count();

/**
 * @brief Defines a Median Aggregation function on a particular field.
 * @return A descriptor of the aggregation function.
 */
API::WindowAggregationPtr Median(const FunctionItem& onField);

/**
 * @brief Defines a Agg Aggregation function on a particular field.
 * @param FunctionItem Attribute("field-name") the field which should be aggregated.
 * @return A descriptor of the aggregation function.
 */
API::WindowAggregationPtr Avg(const FunctionItem& onField);

/**
 * @brief Defines event time as a time characteristic for a window.
 * @param FunctionItem which defines the field name.
 * @return A descriptor of the time characteristic.
 */
Windowing::TimeCharacteristicPtr EventTime(const FunctionItem& onField);

/**
 * @brief Defines event time as a time characteristic for a window.
 * @param FunctionItem which defines the field name.
 * @param Timeunit
 * @return A descriptor of the time characteristic.
 */
Windowing::TimeCharacteristicPtr EventTime(const FunctionItem& onField, const Windowing::TimeUnit& unit);

/**
 * @brief Defines a ingestion time as a time characteristic for a window.
 * @return A descriptor of the time characteristic.
 */
Windowing::TimeCharacteristicPtr IngestionTime();

/**
 * @brief A time measure in Milliseconds.
 * @return TimeMeasure
 */
Windowing::TimeMeasure Milliseconds(uint64_t milliseconds);

/**
 * @brief A time measure in Seconds.
 * @return TimeMeasure
 */
Windowing::TimeMeasure Seconds(uint64_t seconds);

/**
 * @brief A time measure in Minutes.
 * @return TimeMeasure
 */
Windowing::TimeMeasure Minutes(uint64_t minutes);

/**
 * @brief A time measure in Hours.
 * @return TimeMeasure
 */
Windowing::TimeMeasure Hours(uint64_t hours);

/**
 * @brief A time measure in Days.
 * @return TimeMeasure
 */
Windowing::TimeMeasure Days(uint64_t days);

/**
 * @brief A time unit in Milliseconds.
 * @return TimeUnit
 */
Windowing::TimeUnit Milliseconds();

/**
 * @brief A time unit in Seconds.
 * @return TimeUnit
 */
Windowing::TimeUnit Seconds();

/**
 * @brief A time unit in Minutes.
 * @return TimeUnit
 */
Windowing::TimeUnit Minutes();

/**
 * @brief A time unit in Hours.
 * @return TimeUnit
 */
Windowing::TimeUnit Hours();

/**
 * @brief A time unit in Days.
 * @return TimeUnit
 */
Windowing::TimeUnit Days();

/**
 * @brief This function provides access to the creation ts (ingestion ts) of an individual record.
 * This is assigned by the data source.
 * @return NodeFunctionPtr
 */
[[maybe_unused]] NodeFunctionPtr RecordCreationTs();

}
