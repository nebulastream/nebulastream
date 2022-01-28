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

#ifndef NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <string>

namespace NES {

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
    static CpuValues fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& prefix);

    /**
     * @brief Stream operator to convert the object to string
     * @param os
     * @param values
     * @return the stream
     */
    friend std::ostream& operator<<(std::ostream& os, const CpuValues& values);

    /**
     * @brief Returns the metrics as json
     * @return Json containing the metrics
     */
    web::json::value toJson() const;

    bool operator==(const CpuValues& rhs) const;
    bool operator!=(const CpuValues& rhs) const;

    uint64_t user;
    uint64_t nice;
    uint64_t system;
    uint64_t idle;
    uint64_t iowait;
    uint64_t irq;
    uint64_t softirq;
    uint64_t steal;
    uint64_t guest;
    uint64_t guestnice;
} __attribute__((packed));

/**
 * @brief The serialize method to write CpuValues into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the CpuValues
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void writeToBuffer(const CpuValues& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset);

}// namespace NES

#endif  // NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_
