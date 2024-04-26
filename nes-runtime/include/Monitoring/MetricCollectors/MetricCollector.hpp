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

#ifndef NES_RUNTIME_INCLUDE_MONITORING_METRICCOLLECTORS_METRICCOLLECTOR_HPP_
#define NES_RUNTIME_INCLUDE_MONITORING_METRICCOLLECTORS_METRICCOLLECTOR_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/MetricCollectorType.hpp>
#include <memory>

namespace NES::Monitoring {

class Metric;

/**
 * The metric collector base class. Metric collectors are required for the integration with data sources.
 */
class MetricCollector {
  protected:
    //  -- Constructors --
    MetricCollector();
    MetricCollector(WorkerId workerId);
    MetricCollector(const MetricCollector&) = default;
    MetricCollector(MetricCollector&&) = default;
    //  -- Assignment --
    MetricCollector& operator=(const MetricCollector&) = default;
    MetricCollector& operator=(MetricCollector&&) = default;

  public:
    //  -- Destructor --
    virtual ~MetricCollector() = default;

    /**
     * @brief Returns the type of metric collector
     * @return the metric collector type
     */
    virtual MetricCollectorType getType() = 0;

    /**
     * @brief Fill a buffer with a given metric.
     * @param tupleBuffer The tuple buffer
     * @return True if successful, else false
    */
    virtual bool fillBuffer(Runtime::TupleBuffer& tupleBuffer) = 0;

    /**
     * @brief Return the schema representing the metrics gathered by the collector.
     * @return The schema
    */
    virtual SchemaPtr getSchema() = 0;

    /**
     * @brief Fill a buffer with a given metric.
     * @param tupleBuffer The tuple buffer
     * @return True if successful, else false
     */
    virtual const MetricPtr readMetric() const = 0;

    /**
     * Getter for workerId
     * @return the workerId
     */
    WorkerId getWorkerId() const;

    /**
     * Setter for nodeId
     * @param NodeId
     */
    void setNodeId(const WorkerId nodeId);

  private:
    WorkerId workerId;
};

using MetricCollectorPtr = std::shared_ptr<MetricCollector>;

}// namespace NES::Monitoring

#endif // NES_RUNTIME_INCLUDE_MONITORING_METRICCOLLECTORS_METRICCOLLECTOR_HPP_
