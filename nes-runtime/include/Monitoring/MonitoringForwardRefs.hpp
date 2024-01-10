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

#ifndef NES_RUNTIME_INCLUDE_MONITORING_MONITORINGFORWARDREFS_HPP_
#define NES_RUNTIME_INCLUDE_MONITORING_MONITORINGFORWARDREFS_HPP_

#include <Monitoring/Metrics/MetricType.hpp>
#include <Util/MetricCollectorType.hpp>
#include <cstdint>
#include <memory>
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <unordered_map>
#include <vector>

namespace NES {
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

namespace Monitoring {
class MonitoringPlan;
using MonitoringPlanPtr = std::shared_ptr<MonitoringPlan>;

class AbstractSystemResourcesReader;
using AbstractSystemResourcesReaderPtr = std::shared_ptr<AbstractSystemResourcesReader>;

class Metric;
using MetricPtr = std::shared_ptr<Metric>;

class AbstractMetricStore;
using MetricStorePtr = std::shared_ptr<AbstractMetricStore>;

class MonitoringCatalog;
using MonitoringCatalogPtr = std::shared_ptr<MonitoringCatalog>;

class MonitoringManager;
using MonitoringManagerPtr = std::shared_ptr<MonitoringManager>;

using TimestampMetricPtr = std::shared_ptr<std::pair<uint64_t, MetricPtr>>;
using StoredNodeMetricsPtr = std::shared_ptr<std::unordered_map<MetricType, std::shared_ptr<std::vector<TimestampMetricPtr>>>>;

class MetricCollector;
using MetricCollectorPtr = std::shared_ptr<MetricCollector>;
}//namespace Monitoring
}// namespace NES

#endif // NES_RUNTIME_INCLUDE_MONITORING_MONITORINGFORWARDREFS_HPP_
