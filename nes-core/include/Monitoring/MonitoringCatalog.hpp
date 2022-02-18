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

#ifndef NES_INCLUDE_MONITORING_MONITORINGCATALOG_HPP_
#define NES_INCLUDE_MONITORING_MONITORINGCATALOG_HPP_

#include <memory>
#include <unordered_map>
#include <Monitoring/Metrics/MetricType.hpp>

namespace NES {
    class MetricCollector;
    using MetricCollectorPtr = std::shared_ptr<MetricCollector>;
    class MonitoringCatalog;
    using MonitoringCatalogPtr = std::shared_ptr<MonitoringCatalog>;

    class MonitoringCatalog {
      public:
        static MonitoringCatalogPtr create(const std::unordered_map<MetricType, MetricCollectorPtr>&);
        static MonitoringCatalogPtr defaultCatalog();

        MetricCollectorPtr getMetricCollector(MetricType metricCollectorType);

      private:
        explicit MonitoringCatalog(const std::unordered_map<MetricType, MetricCollectorPtr>&);
        std::unordered_map<MetricType, MetricCollectorPtr> metricMap;
    };

}

#endif//NES_INCLUDE_MONITORING_MONITORINGCATALOG_HPP_