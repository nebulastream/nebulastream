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

#ifndef NES_RUNTIME_INCLUDE_MONITORING_METRICCOLLECTORS_DISKCOLLECTOR_HPP_
#define NES_RUNTIME_INCLUDE_MONITORING_METRICCOLLECTORS_DISKCOLLECTOR_HPP_

#include <Monitoring/MetricCollectors/MetricCollector.hpp>
#include <Monitoring/MonitoringForwardRefs.hpp>

namespace NES::Monitoring {

/**
 * The DiskCollector class enables measuring and writing records of class Metrics/Gauge/DiskMetrics into a TupleBuffer.
 */
class DiskCollector : public MetricCollector {
  public:
    explicit DiskCollector();

    /**
     * @brief Returns the type of metric collector
     * @return the metric collector type
     */
    MetricCollectorType getType() override;

    /**
     * @brief Fill a buffer with a given metric.
     * @param tupleBuffer The tuple buffer
     * @return True if successful, else false
     */
    bool fillBuffer(Runtime::TupleBuffer& tupleBuffer) override;

    /**
     * @brief Return the schema representing the metrics gathered by the collector.
     * @return The schema
     */
    SchemaPtr getSchema() override;

    /**
     * @brief Read the disk metrics based on the underlying utility systems reader and return the metrics.
     * @return The metrics object
     */
    const MetricPtr readMetric() const override;

  private:
    AbstractSystemResourcesReaderPtr resourceReader;
    SchemaPtr schema;
};

using DiskCollectorPtr = std::shared_ptr<DiskCollector>;

}// namespace NES::Monitoring

#endif // NES_RUNTIME_INCLUDE_MONITORING_METRICCOLLECTORS_DISKCOLLECTOR_HPP_
