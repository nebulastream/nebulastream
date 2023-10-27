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

#ifndef NES_CORE_INCLUDE_UTIL_CORE_HPP_
#define NES_CORE_INCLUDE_UTIL_CORE_HPP_

#include <API/Schema.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Identifiers.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <any>
#include <functional>
#include <map>
#include <set>
#include <string>

/**
 * @brief a collection of shared utility functions
 */
namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class Query;
using QueryPtr = std::shared_ptr<Query>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

namespace Catalogs {

namespace Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Source

namespace Query {
class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Query

}// namespace Catalogs

namespace Util {

/**
* @brief Outputs a tuple buffer in text format
* @param buffer the tuple buffer
* @return string of tuple buffer
*/
std::string printTupleBufferAsText(Runtime::TupleBuffer& buffer);

/**
 * @brief create CSV lines from the tuples
 * @param tbuffer the tuple buffer
 * @param schema how to read the tuples from the buffer
 * @return a full string stream as string
 */
std::string printTupleBufferAsCSV(Runtime::TupleBuffer tbuffer, const SchemaPtr& schema);

/**
* @brief Returns the physical types of all fields of the schema
* @param schema
* @return PhysicalTypes of the schema's field
*/
std::vector<PhysicalTypePtr> getPhysicalTypes(SchemaPtr schema);

/**
 * @brief method to get the schema as a csv string
 * @param schema
 * @return schema as csv string
 */
std::string toCSVString(const SchemaPtr& schema);

/**
 *
 * @param queryPlan queryIdAndCatalogEntryMapping to which the properties are assigned
 * @param properties properties to assign
 * @return true if the assignment success, and false otherwise
 */
bool assignPropertiesToQueryOperators(const QueryPlanPtr& queryPlan, std::vector<std::map<std::string, std::any>> properties);

/**
 * @brief Creates multiple TupleBuffers from the csv file until the lastTimeStamp has been read
 * @param csvFile
 * @param schema
 * @param timeStampFieldName
 * @param lastTimeStamp
 * @param bufferManager
 * @return Vector of TupleBuffers
 */
[[maybe_unused]] std::vector<Runtime::TupleBuffer> createBuffersFromCSVFile(const std::string& csvFile,
                                                                            const SchemaPtr& schema,
                                                                            Runtime::BufferManagerPtr bufferManager,
                                                                            const std::string& timeStampFieldName,
                                                                            uint64_t lastTimeStamp);
/**
 * @brief Trim leading and trailing whitespace characters from a string.
 *
 * This function removes leading and trailing whitespace characters (space,
 * tab, newline, and carriage return) from the input string.
 *
 * @param str The input string to be trimmed.
 * @return A new string with leading and trailing whitespace removed.
 */
std::string trim(const std::string& str);

}// namespace Util
}// namespace NES

#endif// NES_CORE_INCLUDE_UTIL_CORE_HPP_
