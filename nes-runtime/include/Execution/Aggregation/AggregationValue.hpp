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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_AGGREGATION_AGGREGATIONVALUE_HPP
#define NES_RUNTIME_INCLUDE_EXECUTION_AGGREGATION_AGGREGATIONVALUE_HPP

#include <cstdint>
namespace NES::Runtime::Execution::Aggregation {

/**
 * Base class for aggregation Value
 */
struct AggregationValue {
     //int64_t count; // TODO add count to base class adjust MemRef (sumAggValue = memRefcount + offsite) clean that up and think about it
};

struct AvgAggregationValue : AggregationValue {
    int64_t sum = 0;
    int64_t count = 0;
};

struct SumAggregationValue : AggregationValue {
    int64_t sum = 0;
};

struct CountAggregationValue : AggregationValue {
    int64_t count = 0;
};

struct MinAggregationValue : AggregationValue {
    int64_t min = std::numeric_limits<int64_t>::max();
};

struct MaxAggregationValue : AggregationValue {
    int64_t max = std::numeric_limits<int64_t>::min();
};

}// namespace NES::Runtime::Execution::Aggregation

#endif//NES_SUMAGGREGATIONVALUE_HPP
