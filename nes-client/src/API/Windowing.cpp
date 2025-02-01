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

#include <utility>
#include <API/Functions/Functions.hpp>
#include <API/Windowing.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/AvgAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/CountAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/MaxAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/MedianAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/MinAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/SumAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Synopsis/SamplingWoRDescriptor.hpp>

namespace NES::API
{

WindowAggregation::WindowAggregation(Windowing::WindowAggregationDescriptorPtr windowAggregationDescriptor)
    : aggregation(std::move(windowAggregationDescriptor))
{
}

API::WindowAggregationPtr WindowAggregation::as(const NES::FunctionItem& asField)
{
    return std::make_shared<API::WindowAggregation>(aggregation->as(asField.getNodeFunction()));
}

API::WindowAggregationPtr Sum(const FunctionItem& onField)
{
    return std::make_shared<API::WindowAggregation>(Windowing::SumAggregationDescriptor::on(onField.getNodeFunction()));
}

API::WindowAggregationPtr Avg(const FunctionItem& onField)
{
    return std::make_shared<API::WindowAggregation>(Windowing::AvgAggregationDescriptor::on(onField.getNodeFunction()));
}

API::WindowAggregationPtr Min(const FunctionItem& onField)
{
    return std::make_shared<API::WindowAggregation>(Windowing::MinAggregationDescriptor::on(onField.getNodeFunction()));
}

API::WindowAggregationPtr Max(const FunctionItem& onField)
{
    return std::make_shared<API::WindowAggregation>(Windowing::MaxAggregationDescriptor::on(onField.getNodeFunction()));
}

API::WindowAggregationPtr Count(const FunctionItem& onField)
{
    return std::make_shared<API::WindowAggregation>(Windowing::CountAggregationDescriptor::on(onField.getNodeFunction()));
}

API::WindowAggregationPtr Median(const FunctionItem& onField)
{
    return std::make_shared<API::WindowAggregation>(Windowing::MedianAggregationDescriptor::on(onField.getNodeFunction()));
}

API::WindowAggregationPtr SamplingWoR(const FunctionItem& onField)
{
    return std::make_shared<API::WindowAggregation>(Windowing::SamplingWoRDescriptor::on(onField.getNodeFunction()));
}

Windowing::TimeMeasure Milliseconds(const uint64_t milliseconds)
{
    return Windowing::TimeMeasure(milliseconds);
}

Windowing::TimeMeasure Seconds(const uint64_t seconds)
{
    return Milliseconds(seconds * 1000);
}

Windowing::TimeMeasure Minutes(const uint64_t minutes)
{
    return Seconds(minutes * 60);
}

Windowing::TimeMeasure Hours(const uint64_t hours)
{
    return Minutes(hours * 60);
}

Windowing::TimeMeasure Days(const uint64_t days)
{
    return Hours(days * 24);
}

Windowing::TimeUnit Milliseconds()
{
    return Windowing::TimeUnit::Milliseconds();
}

Windowing::TimeUnit Seconds()
{
    return Windowing::TimeUnit::Seconds();
}

Windowing::TimeUnit Minutes()
{
    return Windowing::TimeUnit::Minutes();
}

Windowing::TimeUnit Hours()
{
    return Windowing::TimeUnit::Hours();
}

Windowing::TimeUnit Days()
{
    return Windowing::TimeUnit::Days();
}

Windowing::TimeCharacteristicPtr EventTime(const FunctionItem& onField)
{
    return Windowing::TimeCharacteristic::createEventTime(onField.getNodeFunction());
}

Windowing::TimeCharacteristicPtr EventTime(const FunctionItem& onField, const Windowing::TimeUnit& unit)
{
    return Windowing::TimeCharacteristic::createEventTime(onField.getNodeFunction(), unit);
}

Windowing::TimeCharacteristicPtr IngestionTime()
{
    return Windowing::TimeCharacteristic::createIngestionTime();
}

NodeFunctionPtr RecordCreationTs()
{
    return Attribute(Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME, BasicType::UINT64).getNodeFunction();
}

}
