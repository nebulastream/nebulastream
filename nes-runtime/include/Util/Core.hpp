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

#include <any>
#include <map>
#include <string>
#include <API/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>

/**
 * @brief a collection of shared utility functions
 */
namespace NES
{

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class Query;
using QueryPtr = std::shared_ptr<Query>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class Operator;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

namespace Catalogs
{

namespace Source
{
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}

namespace Query
{
class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}

}

namespace Util
{

/**
* @brief Outputs a tuple buffer in text format
* @param buffer the tuple buffer
* @return string of tuple buffer
*/
std::string printTupleBufferAsText(Memory::TupleBuffer& buffer);

/**
 * @brief create CSV lines from the tuples
 * @param tbuffer the tuple buffer
 * @param schema how to read the tuples from the buffer
 * @return a full string stream as string
 */
std::string printTupleBufferAsCSV(Memory::TupleBuffer tbuffer, const SchemaPtr& schema);

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
 * @brief Creates a memory layout from the schema and the buffer Size
 * @param schema
 * @param bufferSize
 * @return MemoryLayoutPtr
 */
std::shared_ptr<NES::Memory::MemoryLayouts::MemoryLayout> createMemoryLayout(SchemaPtr schema, uint64_t bufferSize);

/**
 *
 * @param queryPlan queryIdAndCatalogEntryMapping to which the properties are assigned
 * @param properties properties to assign
 * @return true if the assignment success, and false otherwise
 */
bool assignPropertiesToQueryOperators(const QueryPlanPtr& queryPlan, std::vector<std::map<std::string, std::any>> properties);

}
}
