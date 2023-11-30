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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_AGGREGATION_AGGREGATIONVALUE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_AGGREGATION_AGGREGATIONVALUE_HPP_

#include <Execution/Aggregation/Util/HyperLogLog.hpp>
#include <Execution/Aggregation/Util/digestible.h>
#include <cstdint>
#include <numeric>
namespace NES::Runtime::Execution::Aggregation {

/**
 * Base class for aggregation Value
 */
struct AggregationValue {};

/**
 * Class for average aggregation Value, maintains sum and count to calc avg in the lower function
 */
template<typename T>
struct AvgAggregationValue : AggregationValue {
    uint64_t count = 0;
    T sum = 0;
};

/**
 * Class for sum aggregation Value, maintains the sum of all occurred tuples
 */
template<typename T>
struct SumAggregationValue : AggregationValue {
    T sum = 0;
};

/**
 * Class for count aggregation Value, maintains the number of occurred tuples
 */
template<typename T>
struct CountAggregationValue : AggregationValue {
    T count = 0;
};

/**
 * Class for min aggregation Value, maintains the min value of all occurred tuples
 */
template<typename T>
struct MinAggregationValue : AggregationValue {
    T min = std::numeric_limits<T>::max();
};

/**
 * Class for max aggregation Value, maintains the max value of all occurred tuples
 */
template<typename T>
struct MaxAggregationValue : AggregationValue {
    T max = std::numeric_limits<T>::min();
};

/**
 * Class for HyperLogLog Algorithm, maintains the approximate distinct count value of all occurred tuples
 */
struct HyperLogLogDistinctCountApproximationValue : AggregationValue {
    //TODO: #3889 for approximation we require some flexibility to define the size, e.g., appr. window count, etc. to get the best possible result
    hll::HyperLogLog hyperLogLog = hll::HyperLogLog(10);
};

/**
 * Class for quantile aggregation Value, maintains the quantile value of all occurred tuples
 */
struct QuantileEstimationValue : AggregationValue {
    //TODO: #3889 here the same, unsigned is fix, size depends on use case? might be abitrary for this algorithm
    digestible::tdigest<float, unsigned> digest = digestible::tdigest(10);
};

}// namespace NES::Runtime::Execution::Aggregation

#endif  // NES_RUNTIME_INCLUDE_EXECUTION_AGGREGATION_AGGREGATIONVALUE_HPP_
