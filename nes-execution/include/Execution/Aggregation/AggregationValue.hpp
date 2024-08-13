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

#include <cstdint>
#include <numeric>
namespace NES::Runtime::Execution::Aggregation
{

/**
 * Base class for aggregation Value
 */
struct AggregationValue
{
};

/**
 * Class for average aggregation Value, maintains sum and count to calc avg in the lower function
 */
template <typename T>
struct AvgAggregationValue : AggregationValue
{
    uint64_t count = 0;
    T sum = 0;
};

/**
 * Class for sum aggregation Value, maintains the sum of all occurred tuples
 */
template <typename T>
struct SumAggregationValue : AggregationValue
{
    T sum = 0;
};

/**
 * Class for count aggregation Value, maintains the number of occurred tuples
 */
template <typename T>
struct CountAggregationValue : AggregationValue
{
    T count = 0;
};

/**
 * Class for min aggregation Value, maintains the min value of all occurred tuples
 */
template <typename T>
struct MinAggregationValue : AggregationValue
{
    T min = std::numeric_limits<T>::max();
};

/**
 * Class for max aggregation Value, maintains the max value of all occurred tuples
 */
template <typename T>
struct MaxAggregationValue : AggregationValue
{
    T max = std::numeric_limits<T>::min();
};

} /// namespace NES::Runtime::Execution::Aggregation
