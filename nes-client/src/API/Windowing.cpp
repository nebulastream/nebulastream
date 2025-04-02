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

#include <API/Expressions/Expressions.hpp>
#include <API/Windowing.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/AvgAggregationDescriptor.hpp>

#include <Operators/LogicalOperators/Windows/Aggregations/CountAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/MaxAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/MedianAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/MinAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/SumAggregationDescriptor.hpp>
#include <utility>

namespace NES::API {

WindowAggregation::WindowAggregation(Windowing::WindowAggregationDescriptorPtr windowAggregationDescriptor)
    : aggregation(std::move(windowAggregationDescriptor)) {}

API::WindowAggregationPtr WindowAggregation::as(const NES::ExpressionItem& asField) {
    return std::make_shared<API::WindowAggregation>(aggregation->as(asField.getExpressionNode()));
}

API::WindowAggregationPtr Sum(const ExpressionItem& onField) {
    return std::make_shared<API::WindowAggregation>(Windowing::SumAggregationDescriptor::on(onField.getExpressionNode()));
}

API::WindowAggregationPtr Avg(const ExpressionItem& onField) {
    return std::make_shared<API::WindowAggregation>(Windowing::AvgAggregationDescriptor::on(onField.getExpressionNode()));
}

// API::WindowAggregationPtr Variation(const ExpressionItem& onField) {
//     return std::make_shared<API::WindowAggregation>(Windowing::VariAggregationDescriptor::on(onField.getExpressionNode()));
// }


API::WindowAggregationPtr Min(const ExpressionItem& onField) {
    return std::make_shared<API::WindowAggregation>(Windowing::MinAggregationDescriptor::on(onField.getExpressionNode()));
}

API::WindowAggregationPtr Max(const ExpressionItem& onField) {
    return std::make_shared<API::WindowAggregation>(Windowing::MaxAggregationDescriptor::on(onField.getExpressionNode()));
}

API::WindowAggregationPtr Count() {
    return std::make_shared<API::WindowAggregation>(Windowing::CountAggregationDescriptor::on());
}

API::WindowAggregationPtr Median(const ExpressionItem& onField) {
    return std::make_shared<API::WindowAggregation>(Windowing::MedianAggregationDescriptor::on(onField.getExpressionNode()));
}

Windowing::TimeMeasure Milliseconds(uint64_t milliseconds) { return Windowing::TimeMeasure(milliseconds); }

Windowing::TimeMeasure Seconds(uint64_t seconds) { return Milliseconds(seconds * 1000); }

Windowing::TimeMeasure Minutes(uint64_t minutes) { return Seconds(minutes * 60); }

Windowing::TimeMeasure Hours(uint64_t hours) { return Minutes(hours * 60); }

Windowing::TimeMeasure Days(uint64_t days) { return Hours(days); }

Windowing::TimeUnit Milliseconds() { return Windowing::TimeUnit::Milliseconds(); }

Windowing::TimeUnit Seconds() { return Windowing::TimeUnit::Seconds(); }

Windowing::TimeUnit Minutes() { return Windowing::TimeUnit::Minutes(); }

Windowing::TimeUnit Hours() { return Windowing::TimeUnit::Hours(); }

Windowing::TimeUnit Days() { return Windowing::TimeUnit::Days(); }

Windowing::TimeCharacteristicPtr EventTime(const ExpressionItem& onField) {
    return Windowing::TimeCharacteristic::createEventTime(onField.getExpressionNode());
}

Windowing::TimeCharacteristicPtr EventTime(const ExpressionItem& onField, const Windowing::TimeUnit& unit) {
    return Windowing::TimeCharacteristic::createEventTime(onField.getExpressionNode(), unit);
}

Windowing::TimeCharacteristicPtr IngestionTime() { return Windowing::TimeCharacteristic::createIngestionTime(); }

ExpressionNodePtr RecordCreationTs() {
    return Attribute(Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME, BasicType::UINT64).getExpressionNode();
}

}// namespace NES::API
