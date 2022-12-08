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
#include <numeric>
namespace NES::Runtime::Execution::Aggregation {

/**
 * Base class for aggregation Value
 */
struct AggregationValue {
    // TODO 3280: Do we need this? What parameters should be in the base class
};

/**
 * Class for average aggregation Value, maintains sum and count to calc avg in the lower function
 */
struct AvgAggregationValue : AggregationValue {
    int64_t sum = 0;
    int64_t count = 0;
};

/**
 * Class for sum aggregation Value, maintains the sum of all occurred tuples
 */
struct SumAggregationValue : AggregationValue {
    int64_t sum = 0;
};

/**
 * Class for count aggregation Value, maintains the number of occurred tuples
 */
struct CountAggregationValue : AggregationValue {
    int64_t count = 0;
};

/**
 * Class for min aggregation Value, maintains the min value of all occurred tuples
 */
struct MinAggregationValue : AggregationValue {
    // TODO 3280: Take the max from the logical type
    int64_t min = std::numeric_limits<int64_t>::max();
};

/**
 * Class for max aggregation Value, maintains the max value of all occurred tuples
 */
struct MaxAggregationValue : AggregationValue {
    // TODO 3280: Take the min from the logical type
    int64_t max = std::numeric_limits<int64_t>::min();
};

}// namespace NES::Runtime::Execution::Aggregation

#endif//NES_SUMAGGREGATIONVALUE_HPP
