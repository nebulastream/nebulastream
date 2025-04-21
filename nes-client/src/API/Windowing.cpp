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

#include <memory>
#include <utility>
#include <API/Functions/Functions.hpp>
#include <API/Windowing.hpp>
#include <Functions/FunctionExpression.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/AvgAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/CountAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/MaxAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/MedianAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/MinAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/SumAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>

#include <WellKnownDataTypes.hpp>

namespace NES::API
{

WindowAggregation::WindowAggregation(std::shared_ptr<Windowing::WindowAggregationDescriptor> windowAggregationDescriptor)
    : aggregation(std::move(windowAggregationDescriptor))
{
}

std::shared_ptr<API::WindowAggregation> WindowAggregation::as(std::string asField) const
{
    return as(Schema::Identifier{asField, {}});
}

std::shared_ptr<API::WindowAggregation> WindowAggregation::as(Schema::Identifier asField) const
{
    return std::make_shared<API::WindowAggregation>(aggregation->as(std::move(asField)));
}

std::shared_ptr<API::WindowAggregation> Sum(const FunctionItem& onField)
{
    return std::make_shared<API::WindowAggregation>(
        Windowing::SumAggregationDescriptor::create(onField.getNodeFunction(), Schema::Identifier("Sum")));
}

std::shared_ptr<API::WindowAggregation> Avg(const FunctionItem& onField)
{
    return std::make_shared<API::WindowAggregation>(
        Windowing::AvgAggregationDescriptor::create(onField.getNodeFunction(), Schema::Identifier("Avg")));
}

std::shared_ptr<API::WindowAggregation> Min(const FunctionItem& onField)
{
    return std::make_shared<API::WindowAggregation>(
        Windowing::MinAggregationDescriptor::create(onField.getNodeFunction(), Schema::Identifier("Min")));
}

std::shared_ptr<API::WindowAggregation> Max(const FunctionItem& onField)
{
    return std::make_shared<API::WindowAggregation>(
        Windowing::MaxAggregationDescriptor::create(onField.getNodeFunction(), Schema::Identifier("Max")));
}

std::shared_ptr<API::WindowAggregation> Count(const FunctionItem& onField)
{
    return std::make_shared<API::WindowAggregation>(
        Windowing::CountAggregationDescriptor::create(onField.getNodeFunction(), Schema::Identifier("Count")));
}

std::shared_ptr<API::WindowAggregation> Median(const FunctionItem& onField)
{
    return std::make_shared<API::WindowAggregation>(
        Windowing::MedianAggregationDescriptor::create(onField.getNodeFunction(), Schema::Identifier("Median")));
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

std::shared_ptr<Windowing::TimeCharacteristic> EventTime(const FunctionItem& onField)
{
    return Windowing::TimeCharacteristic::createEventTime(onField.getNodeFunction());
}

std::shared_ptr<Windowing::TimeCharacteristic> EventTime(const FunctionItem& onField, const Windowing::TimeUnit& unit)
{
    return Windowing::TimeCharacteristic::createEventTime(onField.getNodeFunction(), unit);
}

std::shared_ptr<Windowing::TimeCharacteristic> IngestionTime()
{
    return Windowing::TimeCharacteristic::createIngestionTime();
}

ExpressionValue RecordCreationTs()
{
    return make_expression<FunctionExpression>(
        {Attribute(Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME).getNodeFunction()}, WellKnown::Integer);
}

}
