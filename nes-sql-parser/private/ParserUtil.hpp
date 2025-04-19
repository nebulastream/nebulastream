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
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>

namespace NES::API
{
Windowing::TimeCharacteristic IngestionTime();
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

}
