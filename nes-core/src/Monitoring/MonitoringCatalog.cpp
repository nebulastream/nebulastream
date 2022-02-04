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

#include <Monitoring/MonitoringCatalog.hpp>
#include <Util/Logger.hpp>

#include <Monitoring/MetricCollectors/DiskCollector.hpp>

namespace NES {

    MonitoringCatalog::MonitoringCatalog(const std::unordered_map<MetricCollectorType, MetricCollectorPtr>& metricCollectors)
        : metricMap(metricCollectors) {
        NES_DEBUG("MonitoringCatalog: Init with collector map of size " << metricCollectors.size());
    }

    MonitoringCatalogPtr
    MonitoringCatalog::create(const std::unordered_map<MetricCollectorType, MetricCollectorPtr>& metricCollectors) {
        return std::make_shared<MonitoringCatalog>(MonitoringCatalog(metricCollectors));
    }

    MonitoringCatalogPtr MonitoringCatalog::defaultCatalog() {
        NES_DEBUG("MonitoringCatalog: Init default catalog");

        std::unordered_map<MetricCollectorType, MetricCollectorPtr> metrics(
            {{MetricCollectorType::DISK_COLLECTOR, std::shared_ptr<MetricCollector>(new DiskCollector())}});
        return create(metrics);
    }

    MetricCollectorPtr MonitoringCatalog::getMetricCollector(MetricCollectorType metricCollectorType) {
        return metricMap.at(metricCollectorType);
    }

}// namespace NES