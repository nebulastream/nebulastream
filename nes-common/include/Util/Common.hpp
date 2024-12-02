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
#include <charconv>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sequencing/SequenceData.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>


namespace NES::Util
{
/**
* @brief escapes all non text characters in a input string, such that the string could be processed as json.
* @param s input string.
* @return result sing.
*/
std::string escapeJson(const std::string& str);

/**
* @brief removes leading and trailing whitespaces
*/
std::string_view trimWhiteSpaces(std::string_view input);

/**
* @brief removes leading and trailing occurences of `trimFor`
*/
std::string_view trimChar(std::string_view in, char trimFor);

namespace detail
{

/**
 * @brief set of helper functions for splitting for different types
 * @return splitting function for a given type
 */
template <typename T>
struct SplitFunctionHelper
{
    /// Most conversions can be delegated to `std::from_chars`
    static constexpr auto FUNCTION = [](std::string_view str)
    {
        T result_value;
        auto trimmed = trimWhiteSpaces(str);
        auto result = std::from_chars(trimmed.data(), trimmed.data() + trimmed.size(), result_value);
        if (result.ec == std::errc::invalid_argument)
        {
            /// TODO #360: This is a fix to such that we do not have to include cpptrace and fmt during parsing.
            throw FunctionNotImplemented("Could not parse: {}", std::string(trimmed));
        }
        return result_value;
    };
};

/**
 * Specialization for `std::string`, which is just a copy from the string_view
 */
template <>
struct SplitFunctionHelper<std::string>
{
    static constexpr auto FUNCTION = [](std::string_view x) { return std::string(x); };
};

}

/// Checks if a string ends with a given string.
bool endsWith(const std::string& fullString, const std::string& ending);

/// Checks if a string starts with a given string.
uint64_t numberOfUniqueValues(std::vector<uint64_t>& values);

/// Get number of unique elements
bool startsWith(const std::string& fullString, const std::string& ending);

/// transforms the string to an upper case version
std::string toUpperCase(std::string string);

/// splits a string given a delimiter into multiple substrings stored in a T vector
/// the delimiter is allowed to be a string rather than a char only.
template <typename T>
std::vector<T> splitWithStringDelimiter(
    std::string_view inputString,
    std::string_view delim,
    std::function<T(std::string_view)> fromStringToT = detail::SplitFunctionHelper<T>::FUNCTION)
{
    size_t prev_pos = 0;
    size_t next_pos = 0;
    std::vector<T> elems;

    while ((next_pos = inputString.find(delim, prev_pos)) != std::string::npos)
    {
        elems.push_back(fromStringToT(inputString.substr(prev_pos, next_pos - prev_pos)));
        prev_pos = next_pos + delim.size();
    }

    if (auto rest = inputString.substr(prev_pos, inputString.size()); !rest.empty())
    {
        elems.push_back(fromStringToT(rest));
    }

    return elems;
}

/// this method checks if the object is null
template <typename T>
std::shared_ptr<T> checkNonNull(std::shared_ptr<T> ptr, const std::string& errorMessage)
{
    NES_ASSERT(ptr, errorMessage);
    return ptr;
}

/// function to replace all string occurrences
void findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr);

/// This function replaces the first occurrence of search term in a string with the replace term.
std::string replaceFirst(std::string origin, const std::string& search, const std::string& replace);

/// Update the source names by sorting and then concatenating the source names from the sub- and query plan
std::string updateSourceName(std::string queryPlanSourceConsumed, std::string subQueryPlanSourceConsumed);

/// Truncates the file and then writes the header string as is to the file
void writeHeaderToCsvFile(const std::string& csvFileName, const std::string& header);

/// Appends the row as is to the csv file
void writeRowToCsvFile(const std::string& csvFileName, const std::string& row);

/// Partition a vector in n chunks, e.g., ([1, 2, 3, 4, 5], 3) -> [[1, 2], [3, 4], [5]]
template <typename T>
std::vector<std::vector<T>> partition(const std::vector<T>& vec, size_t n)
{
    std::vector<std::vector<T>> outVec;
    size_t length = vec.size() / n;
    size_t remain = vec.size() % n;

    size_t begin = 0;
    size_t end = 0;
    for (size_t i = 0; i < std::min(n, vec.size()); ++i)
    {
        end += (remain > 0) ? (length + !!(remain--)) : length;
        outVec.push_back(std::vector<T>(vec.begin() + begin, vec.begin() + end));
        begin = end;
    }
    return outVec;
}

/// appends newValue until the vector contains a minimum of newSize elements
template <typename T>
void padVectorToSize(std::vector<T>& vector, size_t newSize, T newValue)
{
    while (vector.size() < newSize)
    {
        vector.push_back(newValue);
    }
}

/// hashes the key with murmur hash
uint64_t murmurHash(uint64_t key);

/// Counts the number of lines of a string
uint64_t countLines(const std::string& str);

/// Counts the number of lines of a stream, e.g., a file
uint64_t countLines(std::istream& stream);

/// Tries to update curVal until it succeeds or curVal is larger then newVal
template <typename T>
void updateAtomicMax(std::atomic<T>& curVal, const T& newVal)
{
    T prev_value = curVal;
    while (prev_value < newVal && !curVal.compare_exchange_weak(prev_value, newVal))
    {
    }
};

/// check if the given object is an instance of the specified type.
template <typename Out, typename In>
bool instanceOf(const std::shared_ptr<In>& obj)
{
    return std::dynamic_pointer_cast<Out>(obj).get() != nullptr;
}

/// check if the given object is an instance of the specified type.
template <typename Out, typename In>
bool instanceOf(const In& obj)
{
    return dynamic_cast<Out*>(&obj);
}

/// cast the given object to the specified type.
template <typename Out, typename In>
std::shared_ptr<Out> as(const std::shared_ptr<In>& obj)
{
    if (auto ptr = std::dynamic_pointer_cast<Out>(obj))
    {
        return ptr;
    }
    throw InvalidDynamicCast("Invalid dynamic cast: from {} to {}", std::string(typeid(In).name()), std::string(typeid(Out).name()));
}

/// cast the given object to the specified type.
template <typename Out, typename In>
Out as(const In& obj)
{
    return *dynamic_cast<Out*>(&obj);
}

/// cast the given object to the specified type.
template <typename Out, typename In>
std::shared_ptr<Out> as_if(const std::shared_ptr<In>& obj)
{
    return std::dynamic_pointer_cast<Out>(obj);
}

}


namespace folly
{
/// Tries to acquire two locks in a deadlock-free manner. If it fails, it returns an empty optional.
template <class Sync1, class Sync2>
auto tryAcquireLocked(Synchronized<Sync1>& l1, Synchronized<Sync2>& l2)
{
    if (static_cast<const void*>(&l1) < static_cast<const void*>(&l2))
    {
        if (auto locked1 = l1.tryWLock())
        {
            if (auto locked2 = l2.tryWLock())
            {
                return std::optional(std::make_tuple(std::move(locked1), std::move(locked2)));
            }
        }
    }
    else
    {
        if (auto locked2 = l2.tryWLock())
        {
            if (auto locked1 = l1.tryWLock())
            {
                return std::optional(std::make_tuple(std::move(locked1), std::move(locked2)));
            }
        }
    }
    return std::optional<std::tuple<
        LockedPtr<Synchronized<Sync1>, detail::SynchronizedLockPolicyTryExclusive>,
        LockedPtr<Synchronized<Sync2>, detail::SynchronizedLockPolicyTryExclusive>>>{};
}
}
