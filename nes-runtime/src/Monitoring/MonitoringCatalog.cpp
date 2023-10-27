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

#include <Monitoring/MetricCollectors/CpuCollector.hpp>
#include <Monitoring/MetricCollectors/DiskCollector.hpp>
#include <Monitoring/MetricCollectors/MemoryCollector.hpp>
#include <Monitoring/MetricCollectors/NetworkCollector.hpp>
#include <Monitoring/MonitoringCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Monitoring {

MonitoringCatalog::MonitoringCatalog(const std::unordered_map<MetricType, MetricCollectorPtr>& metricCollectors)
    : metricMap(metricCollectors) {
    NES_DEBUG("MonitoringCatalog: Init with collector map of size {}", metricCollectors.size());
}

MonitoringCatalogPtr MonitoringCatalog::create(const std::unordered_map<MetricType, MetricCollectorPtr>& metricCollectors) {
    return std::make_shared<MonitoringCatalog>(MonitoringCatalog(metricCollectors));
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
    NES_ERROR("MonitoringCatalog: MetricType {} is not in catalog.", std::string(magic_enum::enum_name(metricType)));
    return nullptr;
}

}// namespace NES::Monitoring