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

#ifndef NES_INCLUDE_MONITORING_METRICVALUES_CPUMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_CPUMETRICS_HPP_

#include <Monitoring/MetricValues/CpuValues.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <memory>
#include <vector>

namespace NES {
class Schema;
class MonitoringPlan;
typedef std::shared_ptr<Schema> SchemaPtr;

/**
 * @brief Wrapper class to represent the metrics read from the OS about cpu data.
 */
class CpuMetrics {
  public:
    CpuMetrics(CpuValues total, unsigned int size, std::vector<CpuValues>&& arr);

    /**
     * @brief Returns the cpu metrics for a given core
     * @param cpuCore core number
     * @return the cpu metrics
     */
    CpuValues getValues(unsigned int cpuCore) const;

    /**
     * @brief Returns the total cpu metrics summed up across all cores
     * @return The cpu values for all cores
     */
    CpuValues getTotal() const;

    /**
     * @brief The destructor to deallocate space.
     */
    ~CpuMetrics();

    /**
     * @brief Returns the number of cores of the node
     * @return core numbers
     */
    uint16_t getNumCores() const;

    /**
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The object
     */
    static CpuMetrics fromBuffer(SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix);

  private:
    CpuValues total;
    std::vector<CpuValues> cpuValues;
    uint16_t numCores;
};

/**
 * @brief The serialize method to write CpuMetrics into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the CpuMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void serialize(const CpuMetrics& metrics, SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix);

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_CPUMETRICS_HPP_
