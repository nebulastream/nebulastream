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

#ifndef NES_BENCHMARK_INCLUDE_UTIL_BENCHMARKUTILS_HPP_
#define NES_BENCHMARK_INCLUDE_UTIL_BENCHMARKUTILS_HPP_

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Common.hpp>

#include <chrono>

namespace NES::Benchmark::Util {

/**
* @brief creates a vector with a range of [start, stop) and step size
*/
template<typename T>
static void createRangeVector(std::vector<T>& vector, T start, T stop, T stepSize) {
    for (T i = start; i < stop; i += stepSize) {
        vector.push_back(i);
    }
}

/**
 * @brief appends newValue until the vector contains a minimum of newSize elements
 * @tparam T
 * @param vector the vector
 * @param newSize the size of the padded vector
 * @param newValue the value that should be added
 */
template<typename T>
void padVectorToSize(std::vector<T>& vector, size_t newSize, T newValue) {
    while (vector.size() < newSize) {
        vector.push_back(newValue);
    }
}

/**
 * @brief splits the string and fills the vector. If the vector is empty after the filling, the default value is added
 * @tparam T
 * @param vector
 * @param stringToBeSplit
 * @param defaultValue
 */
template<typename T>
static std::vector<T> splitAndFillIfEmpty(const std::string& stringToBeSplit, T defaultValue) {
    auto vec = NES::Util::splitWithStringDelimiter<T>(stringToBeSplit, ",");

    if (vec.empty()) {
        vec.emplace_back(defaultValue);
    }

    return vec;
}

/**
 * @brief creates a vector with a range of [start, stop). This will increase by a power of two. So e.g. 2kb, 4kb, 8kb
 */
template<typename T>
static void createRangeVectorPowerOfTwo(std::vector<T>& vector, T start, T stop) {
    for (T i = start; i < stop; i = i << 1) {
        vector.push_back(i);
    }
}

}// namespace NES::Benchmark::Util
#endif // NES_BENCHMARK_INCLUDE_UTIL_BENCHMARKUTILS_HPP_
