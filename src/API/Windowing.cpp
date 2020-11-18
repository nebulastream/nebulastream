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

#include <API/Expressions/Expressions.hpp>
#include <API/Windowing.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/CountAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MaxAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MinAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>

namespace NES::API {

Windowing::WindowAggregationPtr Sum(ExpressionItem onField) { return Windowing::SumAggregationDescriptor::on(onField); }

Windowing::WindowAggregationPtr Min(ExpressionItem onField) { return Windowing::MinAggregationDescriptor::on(onField); }

Windowing::WindowAggregationPtr Max(ExpressionItem onField) { return Windowing::MaxAggregationDescriptor::on(onField); }

Windowing::WindowAggregationPtr Count() { return Windowing::CountAggregationDescriptor::on(); }

Windowing::TimeMeasure Milliseconds(uint64_t milliseconds) { return Windowing::TimeMeasure(milliseconds); }

Windowing::TimeMeasure Seconds(uint64_t seconds) { return Milliseconds(seconds * 1000); }

Windowing::TimeMeasure Minutes(uint64_t minutes) { return Seconds(minutes * 60); }

Windowing::TimeMeasure Hours(uint64_t hours) { return Minutes(hours * 60); }

Windowing::TimeMeasure Days(uint64_t days) { return Hours(days); }

Windowing::TimeCharacteristicPtr EventTime(ExpressionItem onField) {
    return Windowing::TimeCharacteristic::createEventTime(onField);
}

Windowing::TimeCharacteristicPtr ProcessingTime() { return Windowing::TimeCharacteristic::createProcessingTime(); }

}// namespace NES::API