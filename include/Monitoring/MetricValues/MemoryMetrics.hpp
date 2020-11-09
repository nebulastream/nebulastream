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

#ifndef NES_INCLUDE_MONITORING_METRICVALUES_MEMORYMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_MEMORYMETRICS_HPP_

#include <cstdint>
#include <memory>
#include <string>

namespace NES {
class TupleBuffer;
class Schema;
class MonitoringPlan;
typedef std::shared_ptr<Schema> SchemaPtr;

class MemoryMetrics {
  public:
    MemoryMetrics() = default;

    /**
     * @brief Returns the schema of the class with a given prefix.
     * @param prefix
     * @return the schema
     */
    static SchemaPtr getSchema(const std::string& prefix = "");

    /**
     * @brief Parses a MemoryMetrics objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The MemoryMetrics object
     */
    static MemoryMetrics fromBuffer(SchemaPtr schema, TupleBuffer& buf, const std::string& prefix);

    //equality operators
    bool operator==(const MemoryMetrics& rhs) const;
    bool operator!=(const MemoryMetrics& rhs) const;

    uint64_t TOTAL_RAM;
    uint64_t TOTAL_SWAP;
    uint64_t FREE_RAM;
    uint64_t SHARED_RAM;
    uint64_t BUFFER_RAM;
    uint64_t FREE_SWAP;
    uint64_t TOTAL_HIGH;
    uint64_t FREE_HIGH;
    uint64_t PROCS;
    uint64_t MEM_UNIT;
    uint64_t LOADS_1MIN;
    uint64_t LOADS_5MIN;
    uint64_t LOADS_15MIN;
};

/**
 * @brief The serialize method to write MemoryMetrics into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the MemoryMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void serialize(const MemoryMetrics& metrics, SchemaPtr schema, TupleBuffer& buf, const std::string& prefix);

typedef std::shared_ptr<MemoryMetrics> MemoryMetricsPtr;
}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_MEMORYMETRICS_HPP_
