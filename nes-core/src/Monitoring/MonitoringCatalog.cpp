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

#include <Monitoring/MonitoringCatalog.hpp>
#include <Util/Logger/Logger.hpp>

#include <Monitoring/MetricCollectors/CpuCollector.hpp>
#include <Monitoring/MetricCollectors/DiskCollector.hpp>
#include <Monitoring/MetricCollectors/MemoryCollector.hpp>
#include <Monitoring/MetricCollectors/NetworkCollector.hpp>

namespace NES::Monitoring {

MonitoringCatalog::MonitoringCatalog(const std::unordered_map<MetricType, MetricCollectorPtr>& metricCollectors)
    : metricMap(metricCollectors) {
    NES_DEBUG("MonitoringCatalog: Init with collector map of size " << metricCollectors.size());
}

MonitoringCatalogPtr MonitoringCatalog::create(const std::unordered_map<MetricType, MetricCollectorPtr>& metricCollectors) {
    return std::make_shared<MonitoringCatalog>(MonitoringCatalog(metricCollectors));
}

MonitoringCatalogPtr MonitoringCatalog::createCatalog(const MonitoringPlanPtr& monitoringPlan) {
    NES_DEBUG("MonitoringCatalog: Init catalog for Monitoringplan!");

    std::unordered_map<MetricType, MetricCollectorPtr> metrics;
    if(monitoringPlan->hasMetric(WrappedCpuMetrics)) {
        metrics.insert({MetricType::WrappedCpuMetrics,
                        std::shared_ptr<MetricCollector>(new CpuCollector(monitoringPlan->getSchema(WrappedCpuMetrics)))});
    }
    if (monitoringPlan->hasMetric(DiskMetric)) {
        metrics.insert({MetricType::DiskMetric,
                        std::shared_ptr<MetricCollector>(new DiskCollector(monitoringPlan->getSchema(DiskMetric)))});
    }
    if (monitoringPlan->hasMetric(MemoryMetric)) {
        metrics.insert({MetricType::MemoryMetric,
                        std::shared_ptr<MetricCollector>(new MemoryCollector(monitoringPlan->getSchema(MemoryMetric)))});
    }
    if (monitoringPlan->hasMetric(WrappedNetworkMetrics)) {
        metrics.insert({MetricType::WrappedNetworkMetrics,
                        std::shared_ptr<MetricCollector>(new NetworkCollector(monitoringPlan->getSchema(WrappedNetworkMetrics)))});
    }
    // TODO: add Registration and Runtime Metrics

    return create(metrics);
}

MonitoringCatalogPtr MonitoringCatalog::defaultCatalog() {
    NES_DEBUG("MonitoringCatalog: Init default catalog");

    std::unordered_map<MetricType, MetricCollectorPtr> metrics(
        {{MetricType::WrappedCpuMetrics, std::shared_ptr<MetricCollector>(new CpuCollector())},
         {MetricType::DiskMetric, std::shared_ptr<MetricCollector>(new DiskCollector())},
         {MetricType::MemoryMetric, std::shared_ptr<MetricCollector>(new MemoryCollector())},
         {MetricType::WrappedNetworkMetrics, std::shared_ptr<MetricCollector>(new NetworkCollector())}});
    return create(metrics);
}

MetricCollectorPtr MonitoringCatalog::getMetricCollector(MetricType metricType) {
    if (metricMap.contains(metricType)) {
        return metricMap[metricType];
    }
    NES_ERROR("MonitoringCatalog: MetricType " << toString(metricType) << " is not in catalog.");
    return nullptr;
}

}// namespace NES::Monitoring