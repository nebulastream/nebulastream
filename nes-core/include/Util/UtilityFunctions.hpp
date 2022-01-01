/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_UTIL_UTILITY_FUNCTIONS_HPP_
#define NES_INCLUDE_UTIL_UTILITY_FUNCTIONS_HPP_

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Operators/OperatorId.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <functional>
#include <map>
#include <string>

/**
 * @brief a collection of shared utility functions
 */
namespace NES {

class StreamCatalog;
using StreamCatalogPtr = std::shared_ptr<StreamCatalog>;

class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;

class Query;
using QueryPtr = std::shared_ptr<Query>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

namespace Util {
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
bool startsWith(const std::string& fullString, const std::string& ending);

/**
   * @brief this function creates an ID string
   * @return the ID string
   * https://stackoverflow.com/questions/24365331/how-can-i-generate-uuid-in-c-without-using-boost-library
   * example id: 665b3caf-f097-568e-2b2f-220b6c0a9bcd
   */
std::string generateIdString();

/**
   * @brief this function creates a hased ID as int
   * @return the ID as site_t
   */
std::uint64_t generateIdInt();

/**
   * @brief get the first substring between a unique first delimiter and a non-unique second delimiter
   * @param input
   * @param delimiter1
   * @param delimiter2
   * @return the substring
   */
std::string getFirstStringBetweenTwoDelimiters(const std::string& input, const std::string& str1, const std::string& str2);

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
   * @brief Outputs a tuple buffer accordingly to a specific schema
   * @param buffer the tuple buffer
   * @param schema  the schema
   * @return
   */

std::string prettyPrintTupleBuffer(Runtime::TupleBuffer& buffer, const SchemaPtr& schema);

/**
   * @brief Outputs a tuple buffer in text format
   * @param buffer the tuple buffer
   * @return string of tuple buffer
   */
std::string printTupleBufferAsText(Runtime::TupleBuffer& buffer);

/**
    * @brief this method creates a string from the content of a tuple buffer
    * @return string of the buffer content
    */
std::string printTupleBufferAsCSV(Runtime::TupleBuffer tbuffer, const SchemaPtr& schema);

/**
    * @brief this method creates a string from the content of a tuple buffer
    * @return string of the buffer content
    */
std::string castStringToBasicTypeAndWriteToBuffer(BasicPhysicalType::NativeType type, Runtime::TupleBuffer& tbuffer);

/**
     * @brief function to replace all string occurrences
     * @param data input string will be replaced in-place
     * @param toSearch search string
     * @param replaceStr replace string
     */
void findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr);

/**
     * @brief method to get the schema as a csv string
     * @param schema
     * @return schema as csv string
     */
std::string toCSVString(const SchemaPtr& schema);

/**
     * @brief Returns the next free operator id
     * @return operator id
     */
OperatorId getNextOperatorId();

/**
   * @brief Returns the next free pipeline id
   * @return node id
   */
uint64_t getNextPipelineId();

/**
     *
     * @brief This function replaces the first occurrence of search term in a string with the replace term.
     * @param origin - The original string that is to be manipulated
     * @param search - The substring/term which we want to have replaced
     * @param replace - The string that is replacing the search term.
     * @return
     */
std::string replaceFirst(std::string origin, const std::string& search, const std::string& replace);

/**
     *
     * @param queryPlan queries to which the properties are assigned
     * @param properties properties to assign
     * @return true if the assignment success, and false otherwise
     */
bool assignPropertiesToQueryOperators(const QueryPlanPtr& queryPlan, std::vector<std::map<std::string, std::any>> properties);

};// namespace Util
}// namespace NES

#endif// NES_INCLUDE_UTIL_UTILITY_FUNCTIONS_HPP_
