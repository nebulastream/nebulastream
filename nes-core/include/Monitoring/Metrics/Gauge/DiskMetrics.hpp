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

#ifndef NES_INCLUDE_MONITORING_METRICVALUES_DISKMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_DISKMETRICS_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES {

/**
 * @brief DiskMetrics class, that is responsible for collecting and managing disk metrics.
 */
class DiskMetrics {
  public:
    DiskMetrics() = default;

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
    void writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const;

    /**
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The object
    */
    void readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex);

    /**
     * @brief Returns the metrics as json
     * @return Json containing the metrics
     */
    [[nodiscard]] web::json::value toJson() const;

    bool operator==(const DiskMetrics& rhs) const;
    bool operator!=(const DiskMetrics& rhs) const;

    uint64_t fBsize;
    uint64_t fFrsize;
    uint64_t fBlocks;
    uint64_t fBfree;
    uint64_t fBavail;
} __attribute__((packed));

using DiskMetricsPtr = std::shared_ptr<DiskMetrics>;

/**
 * @brief The serialize method to write metrics into the given Schema and TupleBuffer.
 * @param the metrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void writeToBuffer(const DiskMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses metrics objects from a given Schema and TupleBuffer.
 * @param schema
 * @param buf
 * @param prefix
 * @return The object
*/
void readFromBuffer(DiskMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

}// namespace NES

#endif// NES_INCLUDE_MONITORING_METRICVALUES_DISKMETRICS_HPP_
