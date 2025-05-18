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
#include <memory>
#include <ostream>
#include <string>
#include <vector>
#include <ErrorHandling.hpp>

namespace NES
{
/// Used by all `Node`s to control the verbosity of their `operator<<` method.
enum class VerbosityLevel : uint8_t
{
    /// Print everything.
    Debug = 0,
    /// Print only the name of the Node/Function, to make the queryplan printing easier to read.
    QueryPlan = 1,
};
/// Check the iword array of the passed stream for the set `VerbosityLevel` using the allocated index.
VerbosityLevel getVerbosityLevel(std::ostream& os);
/// Set the iword array of the passed stream to the passed `VerbosityLevel` using the allocated index.
void setVerbosityLevel(std::ostream& os, const VerbosityLevel& level);
/// The index for the `VerbosityLevel` into any stream's iword array.
/// The index may only be allocated once, otherwise it will change and we won't be able to set and retrieve it correctly.
int getIwordIndex();

inline std::ostream& operator<<(std::ostream& os, const VerbosityLevel& level)
{
    setVerbosityLevel(os, level);
    return os;
}
}

namespace NES::Util
{
/// @brief escapes all non text characters in a input string, such that the string could be processed as json.
std::string escapeJson(const std::string& str);

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

template <typename Derived, typename Base>
bool instanceOf(const std::unique_ptr<Base>& ptr)
{
    return dynamic_cast<const Derived*>(ptr.get()) != nullptr;
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


template <typename T, typename S>
std::unique_ptr<T> dynamic_pointer_cast(std::unique_ptr<S>&& ptr) noexcept
{
    if (auto* converted = dynamic_cast<T*>(ptr.get()))
    {
        ptr.release();
        return std::unique_ptr<T>(converted);
    }
    return nullptr;
}
}
