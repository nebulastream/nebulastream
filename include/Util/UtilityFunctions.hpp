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

#ifndef UTILITY_FUNCTIONS_HPP
#define UTILITY_FUNCTIONS_HPP

#include <Operators/OperatorId.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <cpprest/json.h>
#include <map>
#include <string>

/*
- * The above undef ensures that NES will compile.
- * There is a 3rd-party library that defines U as a macro for some internal stuff.
- * U is also a template argument of a template function in boost.
- * When the compiler sees them both, it goes crazy.
- * Do not remove the above undef.
- */
#undef U

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

class UtilityFunctions {
  public:
    /**
     * @brief escapes all non text characters in a input string, such that the string could be processed as json.
     * @param s input string.
     * @return result sing.
     */
    static std::string escapeJson(const std::string& s);

    /**
     * @brief removes leading and trailing whitespaces
     */
    static std::string trim(std::string s);

    /**
     * @brief Checks if a string ends with a given string.
     * @param fullString
     * @param ending
     * @return true if it ends with the given string, else false
     */
    static bool endsWith(const std::string& fullString, const std::string& ending);

    /**
     * @brief Checks if a string starts with a given string.
     * @param fullString
     * @param start
     * @return true if it ends with the given string, else false
     */
    static bool startsWith(const std::string& fullString, const std::string& ending);

    /**
   * @brief this function creates an ID string
   * @return the ID string
   * https://stackoverflow.com/questions/24365331/how-can-i-generate-uuid-in-c-without-using-boost-library
   * example id: 665b3caf-f097-568e-2b2f-220b6c0a9bcd
   */
    static std::string generateIdString();

    /**
   * @brief this function creates a hased ID as int
   * @return the ID as site_t
   */
    static std::uint64_t generateIdInt();

    /**
   * @brief get the first substring between a unique first delimiter and a non-unique second delimiter
   * @param input
   * @param delimiter1
   * @param delimiter2
   * @return the substring
   */
    static std::string getFirstStringBetweenTwoDelimiters(const std::string& input, const std::string& s1, const std::string& s2);

    /**
   * @brief Outputs a tuple buffer accordingly to a specific schema
   * @param buffer the tuple buffer
   * @param schema  the schema
   * @return
   */

    /**
    * @brief splits a string given a delimiter into multiple substrings stored in a std::string vector
    * the delimiter is allowed to be a string rather than a char only.
    * @param data - the string that is to be split
    * @param delimiter - the string that is to be split upon e.g. / or -
    * @return
    */
    static std::vector<std::string> splitWithStringDelimiter(std::string& inputString, const std::string& delim);

    /**
    * @brief splits a string given a delimiter into multiple unsigned integer stored in a std::uint64_t vector
    * the delimiter is allowed to be a string rather than a char only.
    * @param data - the string that is to be split
    * @param delimiter - the string that is to be split upon e.g. / or -
    * @return
    */
    static std::vector<std::uint64_t> splitWithStringDelimiterAsInt(const std::string& inputString, const std::string& delim);

    static std::string prettyPrintTupleBuffer(Runtime::TupleBuffer& buffer, const SchemaPtr& schema);

    /**
   * @brief Outputs a tuple buffer in text format
   * @param buffer the tuple buffer
   * @return string of tuple buffer
   */
    static std::string printTupleBufferAsText(Runtime::TupleBuffer& buffer);

    /**
    * @brief this method creates a string from the content of a tuple buffer
    * @return string of the buffer content
    */
    static std::string printTupleBufferAsCSV(Runtime::TupleBuffer& tbuffer, const SchemaPtr& schema);

    /**
      * @brief function to obtain JSON representation of a NES Topology
      * @param root of the Topology
      * @return JSON representation of the Topology
      */
    static web::json::value getTopologyAsJson(TopologyNodePtr root);

    /**
     * @brief function to replace all string occurrences
     * @param data input string will be replaced in-place
     * @param toSearch search string
     * @param replaceStr replace string
     */
    static void findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr);

    /**
     * @brief method to get the schema as a csv string
     * @param schema
     * @return schema as csv string
     */
    static std::string toCSVString(const SchemaPtr& schema);

    /**
     * @brief Returns the next free operator id
     * @return operator id
     */
    static OperatorId getNextOperatorId();

    /**
   * @brief Returns the next free pipeline id
   * @return node id
   */
    static uint64_t getNextPipelineId();

    /**
     * @brief Returns the next free node id
     * @return node id
     */
    static uint64_t getNextTopologyNodeId();

    /**
     * @brief Returns the next free node id
     * @return node id
     */
    static uint64_t getNextNodeEngineId();

    /**
     * @brief Returns the next free task id
     * @return node id
     */
    static uint64_t getNextTaskId();

    /**
     *
     * @brief This function replaces the first occurrence of search term in a string with the replace term.
     * @param origin - The original string that is to be manipulated
     * @param search - The substring/term which we want to have replaced
     * @param replace - The string that is replacing the search term.
     * @return
     */
    static std::string replaceFirst(std::string origin, const std::string& search, const std::string& replace);

    /**
     *
     * @param queryPlan queries to which the properties are assigned
     * @param properties properties to assign
     * @return true if the assignment success, and false otherwise
     */
    static bool assignPropertiesToQueryOperators(const QueryPlanPtr& queryPlan,
                                                 std::vector<std::map<std::string, std::any>> properties);

  private:
    /**
     * @brief This method is responsible for returning the location where the sub query terminates
     * @param startOfUnionWith : the location where unionWith starts
     * @param queryCodeSnippet: the whole string code snippet
     * @return location where sub query terminates
     */
    static uint64_t findSubQueryTermination(uint64_t startOfUnionWith, const std::string& queryCodeSnippet);
};
}// namespace NES

#endif /* UTILITY_FUNCTIONS_HPP */
