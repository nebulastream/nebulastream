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

#ifndef NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_

#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <memory>
namespace NES {
class Schema;
class MonitoringPlan;
typedef std::shared_ptr<Schema> SchemaPtr;

/**
 * @brief This class represents the metrics read from /proc/stat.
 */
class CpuValues {
  public:
    CpuValues() = default;

    /**
     * @brief Returns the schema of the class with a given prefix.
     * @param prefix
     * @return the schema
     */
    static SchemaPtr getSchema(const std::string& prefix);

    /**
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The object
     */
    static CpuValues fromBuffer(SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix);

    uint64_t USER;
    uint64_t NICE;
    uint64_t SYSTEM;
    uint64_t IDLE;
    uint64_t IOWAIT;
    uint64_t IRQ;
    uint64_t SOFTIRQ;
    uint64_t STEAL;
    uint64_t GUEST;
    uint64_t GUESTNICE;
};

/**
 * @brief The serialize method to write CpuValues into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the CpuValues
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void serialize(const CpuValues& metrics, SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix);

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_
