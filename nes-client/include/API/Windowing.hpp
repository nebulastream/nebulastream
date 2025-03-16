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
#include <API/Functions/Functions.hpp>
#include <Functions/NodeFunction.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Measures/TimeUnit.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>

/**
 * @brief The following declares API functions for windowing.
 */
namespace NES::API
{

class WindowAggregation
{
public:
    explicit WindowAggregation(std::shared_ptr<Windowing::WindowAggregationDescriptor> windowAggregationDescriptor);
    [[nodiscard]] std::shared_ptr<API::WindowAggregation> as(const FunctionItem& asField) const;
    std::shared_ptr<Windowing::WindowAggregationDescriptor> aggregation;
};

std::shared_ptr<API::WindowAggregation> Sum(const FunctionItem& onField);
std::shared_ptr<API::WindowAggregation> Min(const FunctionItem& onField);
std::shared_ptr<API::WindowAggregation> Max(const FunctionItem& onField);
std::shared_ptr<API::WindowAggregation> Count(const FunctionItem& onField);
std::shared_ptr<API::WindowAggregation> Median(const FunctionItem& onField);
std::shared_ptr<API::WindowAggregation> Avg(const FunctionItem& onField);
std::shared_ptr<Windowing::TimeCharacteristic> EventTime(const FunctionItem& onField);
std::shared_ptr<Windowing::TimeCharacteristic> EventTime(const FunctionItem& onField, const Windowing::TimeUnit& unit);
std::shared_ptr<Windowing::TimeCharacteristic> IngestionTime();
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
[[maybe_unused]] std::shared_ptr<NodeFunction> RecordCreationTs();

}
