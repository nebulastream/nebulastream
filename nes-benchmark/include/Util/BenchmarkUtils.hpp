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

#ifndef NES_BENCHMARKUTILS_HPP
#define NES_BENCHMARKUTILS_HPP

#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Common/Identifiers.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

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

namespace detail {
/**
* @brief set of helper functions for splitting for different types
* @return splitting function for a given type
*/
template<typename T>
struct SplitFunctionHelper {};

template<>
struct SplitFunctionHelper<std::string> {
    static constexpr auto FUNCTION = [](std::string x) {
        return x;
    };
};

template<>
struct SplitFunctionHelper<uint64_t> {
    static constexpr auto FUNCTION = [](std::string&& str) {
        return uint64_t(std::atoll(str.c_str()));
    };
};

template<>
struct SplitFunctionHelper<uint32_t> {
    static constexpr auto FUNCTION = [](std::string&& str) {
        return uint32_t(std::atoi(str.c_str()));
    };
};

template<>
struct SplitFunctionHelper<int> {
    static constexpr auto FUNCTION = [](std::string&& str) {
        return std::atoi(str.c_str());
    };
};

template<>
struct SplitFunctionHelper<double> {
    static constexpr auto FUNCTION = [](std::string&& str) {
        return std::atof(str.c_str());
    };
};

}// namespace detail

///**
//* @brief splits a string given a delimiter into multiple substrings stored in a T vector
//* the delimiter is allowed to be a string rather than a char only.
//* @param data - the string that is to be split
//* @param delimiter - the string that is to be split upon e.g. / or -
//* @param fromStringtoT - the function that converts a string to an arbitrary type T
//* @return
//*/
//template<typename T>
//std::vector<T> splitWithStringDelimiter(const std::string& inputString,
//                                        const std::string& delim,
//                                        std::function<T(std::string)> fromStringToT = detail::SplitFunctionHelper<T>::FUNCTION) {
//    std::string copy = inputString;
//    size_t pos = 0;
//    std::vector<T> elems;
//    while ((pos = copy.find(delim)) != std::string::npos) {
//        elems.push_back(fromStringToT(copy.substr(0, pos)));
//        copy.erase(0, pos + delim.length());
//    }
//    if (!copy.substr(0, pos).empty()) {
//        elems.push_back(fromStringToT(copy.substr(0, pos)));
//    }
//
//    return elems;
//}

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

#endif//NES_BENCHMARKUTILS_HPP
