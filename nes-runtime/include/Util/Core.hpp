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
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <API/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

/**
 * @brief a collection of shared utility functions
 */
namespace NES
{

namespace Util
{

/**
* @brief Outputs a tuple buffer in text format
* @param buffer the tuple buffer
* @return string of tuple buffer
*/
std::string printTupleBufferAsText(Memory::PinnedBuffer& buffer);

/**
 * @brief create CSV lines from the tuples
 * @param tbuffer the tuple buffer
 * @param schema how to read the tuples from the buffer
 * @return a full string stream as string
 */
std::string printTupleBufferAsCSV(Memory::PinnedBuffer tbuffer, const std::shared_ptr<Schema>& schema);

/**
* @brief Returns the physical types of all fields of the schema
* @param schema
* @reconst turn PhysicalTypes of t&he schema's field
*/
std::vector<std::shared_ptr<PhysicalType>> getPhysicalTypes(const Schema& schema);

/**
 * @brief method to get the schema as a csv string
 * @param schema
 * @return schema as csv string
 */
std::string toCSVString(const Schema& schema);

/**
 * @brief Creates a memory layout from the schema and the buffer Size
 * @param schema
 * @param bufferSize
 * @const return MemoryLayoutPtr
& */
std::shared_ptr<NES::Memory::MemoryLayouts::MemoryLayout> createMemoryLayout(const std::shared_ptr<Schema>& schema, uint64_t bufferSize);

/**
 *
 * @param queryPlan queryIdAndCatalogEntryMapping to which the properties are assigned
 * @param properties properties to assign
 * @return true if the assignment success, and false otherwise
 */
bool assignPropertiesToQueryOperators(const std::shared_ptr<QueryPlan>& queryPlan, std::vector<std::map<std::string, std::any>> properties);

}
}
