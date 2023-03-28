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

#ifndef NES_COMMON_INCLUDE_UTIL_COMMON_HPP_
#define NES_COMMON_INCLUDE_UTIL_COMMON_HPP_

#include <Common/Identifiers.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <any>
#include <complex>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace NES::QueryCompilation {
enum class StreamJoinStrategy : uint8_t {
    HASH_JOIN_LOCAL,
    HASH_JOIN_GLOBAL_LOCKING,
    HASH_JOIN_GLOBAL_LOCK_FREE,
    NESTED_LOOP_JOIN
};

enum class JoinBuildSideType : uint8_t { Right, Left };
template<typename E = JoinBuildSideType, typename Out = uint64_t>
constexpr Out to_underlying(E e) noexcept {
    return static_cast<Out>(e);
}
}// namespace NES::QueryCompilation

namespace NES::Util {
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

/**
* @brief escapes all non text characters in a input string, such that the string could be processed as json.
* @param s input string.
* @return result sing.
*/
std::string escapeJson(const std::string& str);

/**
* @brief removes leading and trailing whitespaces
*/
std::string trim(std::string s);

/**
* @brief removes leading and trailing characters of trimFor
*/
std::string trim(std::string s, char trimFor);

/**
* @brief Checks if a string ends with a given string.
* @param fullString
* @param ending
* @return true if it ends with the given string, else false
*/
bool endsWith(const std::string& fullString, const std::string& ending);

/**
* @brief Checks if a string starts with a given string.
* @param fullString
* @param start
* @return true if it ends with the given string, else false
*/
uint64_t numberOfUniqueValues(std::vector<uint64_t>& values);

/**
* @brief Get number of unique elements
* @param fullString
* @param start
* @return true if it ends with the given string, else false
*/
bool startsWith(const std::string& fullString, const std::string& ending);

/**
* @brief transforms the string to an upper case version
* @param string
* @return string
*/
std::string toUpperCase(std::string string);

/**
* @brief splits a string given a delimiter into multiple substrings stored in a T vector
* the delimiter is allowed to be a string rather than a char only.
* @param data - the string that is to be split
* @param delimiter - the string that is to be split upon e.g. / or -
* @param fromStringtoT - the function that converts a string to an arbitrary type T
* @return
*/
template<typename T>
std::vector<T> splitWithStringDelimiter(const std::string& inputString,
                                        const std::string& delim,
                                        std::function<T(std::string)> fromStringToT = detail::SplitFunctionHelper<T>::FUNCTION) {
    std::string copy = inputString;
    size_t pos = 0;
    std::vector<T> elems;
    while ((pos = copy.find(delim)) != std::string::npos) {
        elems.push_back(fromStringToT(copy.substr(0, pos)));
        copy.erase(0, pos + delim.length());
    }
    if (!copy.substr(0, pos).empty()) {
        elems.push_back(fromStringToT(copy.substr(0, pos)));
    }

    return elems;
}

/**
* @brief this method checks if the object is null
* @return pointer to the object
*/
template<typename T>
std::shared_ptr<T> checkNonNull(std::shared_ptr<T> ptr, const std::string& errorMessage) {
    NES_ASSERT(ptr, errorMessage);
    return ptr;
}

/**
* @brief function to replace all string occurrences
* @param data input string will be replaced in-place
* @param toSearch search string
* @param replaceStr replace string
*/
void findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr);
/**
* @brief This function replaces the first occurrence of search term in a string with the replace term.
* @param origin - The original string that is to be manipulated
* @param search - The substring/term which we want to have replaced
* @param replace - The string that is replacing the search term.
* @return
*/
std::string replaceFirst(std::string origin, const std::string& search, const std::string& replace);

/**
 * @brief: Update the source names by sorting and then concatenating the source names from the sub- and query plan
 * @param string consumed sources of the current queryPlan
 * @param string consumed sources of the subQueryPlan
 * @return string with new source name
 */
std::string updateSourceName(std::string queryPlanSourceConsumed, std::string subQueryPlanSourceConsumed);

/**
* @brief Truncates the file and then writes the header string as is to the file
* @param csvFileName
* @param header
*/
void writeHeaderToCsvFile(const std::string& csvFileName, const std::string& header);

/**
* @brief Appends the row as is to the csv file
* @param csvFileName
* @param row
*/
void writeRowToCsvFile(const std::string& csvFileName, const std::string& row);

/**
* Partition a vector in n chunks, e.g., ([1, 2, 3, 4, 5], 3) -> [[1, 2], [3, 4], [5]]
* @param input the vector
* @param n the chunks
* @return the chunked vector
*/
template<typename T>
std::vector<std::vector<T>> partition(const std::vector<T>& vec, size_t n) {
    std::vector<std::vector<T>> outVec;
    size_t length = vec.size() / n;
    size_t remain = vec.size() % n;

    size_t begin = 0;
    size_t end = 0;
    for (size_t i = 0; i < std::min(n, vec.size()); ++i) {
        end += (remain > 0) ? (length + !!(remain--)) : length;
        outVec.push_back(std::vector<T>(vec.begin() + begin, vec.begin() + end));
        begin = end;
    }
    return outVec;
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
* @brief Performs fft on a vector
* @return true/false for now
*/
std::vector<std::complex<double>> fft(const std::vector<double>& lastWindowValues);

/**
* @brief Performs fftfreq given a size and a step in freq.
* @return the set of FFT bins
*/
std::vector<double> fftfreq(const int n, const double d=1.);

/**
* @brief Calculates the PSD from an FFT.
* @return the fft squared
*/
std::vector<double> psd(const std::vector<std::complex<double>>& fftValues);

/**
* @brief Calculates the total energy from an FFT.
* @return the sum of energies across all frequencies
*/
double totalEnergy(const std::vector<std::complex<double>>& fftValues);

/**
 * @brief Check the PSD of a signal if it's aliased and its nyq. freq.
 * @param psd_array
 * @param total_energy sum of signal's energy levels
 * @return 2-tuple of is_aliased and the proposed nyq. rate
 */
std::tuple<bool, int> is_aliased_and_nyq_freq(const std::vector<double>& psd_array, double total_energy);

/**
 * @brief Check a window of a signal, infer its nyq. and decide if it's aliased/non-aliased.
 * @param inputSignal the input signal of the window
 * @param interval the current avg. of the interval in the window
 * @return 2-tuple of (true/false if oversampled), proposed nyquist in s)
 */
std::tuple<bool, double> computeNyquistAndEnergy(const std::vector<double>& inputSignal, double intervalInSeconds);

};// namespace Util
}// namespace NES

#endif// NES_COMMON_INCLUDE_UTIL_COMMON_HPP_
