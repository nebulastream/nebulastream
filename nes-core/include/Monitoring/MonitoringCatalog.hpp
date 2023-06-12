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

#ifndef NES_CORE_INCLUDE_MONITORING_MONITORINGCATALOG_HPP_
#define NES_CORE_INCLUDE_MONITORING_MONITORINGCATALOG_HPP_

#include <Monitoring/Metrics/MetricType.hpp>
#include <memory>
#include <unordered_map>

namespace NES::Monitoring {
class MetricCollector;
using MetricCollectorPtr = std::shared_ptr<MetricCollector>;
class MonitoringCatalog;
using MonitoringCatalogPtr = std::shared_ptr<MonitoringCatalog>;

/**
 * The MonitoringCatalog which is responsible for mapping MetricTypes to the corresponding MetricCollector.
 */
class MonitoringCatalog {
  public:
    /**
     * Creates a catalog based on a given map.
     * @param metricCollectors a map from MetricType to MetricCollector
     * @return the catalog
     */
    static MonitoringCatalogPtr create(const std::unordered_map<MetricType, MetricCollectorPtr>& metricCollectors);

    /**
     * Creates a MonitoringCatalog with a pre-initialized map that contains basic MetricTypes and their collectors.
     * @return the catalog
     */
    static MonitoringCatalogPtr defaultCatalog();

    /**
     * Get the collector for a specific MetricType
     * @param metricType
     * @return the ptr to MetricCollector
     */
    MetricCollectorPtr getMetricCollector(MetricType metricType);

  private:
    explicit MonitoringCatalog(const std::unordered_map<MetricType, MetricCollectorPtr>&);
    std::unordered_map<MetricType, MetricCollectorPtr> metricMap;
};

}// namespace NES::Monitoring

#endif// NES_CORE_INCLUDE_MONITORING_MONITORINGCATALOG_HPP_
