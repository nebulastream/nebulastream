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

#include <Util/Logger.hpp>
#include <Monitoring/MonitoringPlan.hpp>

namespace NES {
    MonitoringPlan::MonitoringPlan(const std::set<MetricCollectorType>& metrics) : metricTypes(metrics) {
        NES_DEBUG("MonitoringPlan: Init with metrics of size " << metrics.size());
    }

    MonitoringPlan::MonitoringPlan(const SerializableMonitoringPlan& plan) {
        NES_DEBUG("MonitoringPlan: Initializing from shippable protobuf object.");
        for (auto serMetricType : plan.metrictypes()) {
            addMetric(static_cast<MetricCollectorType>(serMetricType));
        }
    }

    MonitoringPlanPtr MonitoringPlan::create(const std::set<MetricCollectorType>& metrics) {
        return std::shared_ptr<MonitoringPlan>(new MonitoringPlan(metrics));
    }

    MonitoringPlanPtr MonitoringPlan::create(const SerializableMonitoringPlan& shippable) {
        return std::shared_ptr<MonitoringPlan>(new MonitoringPlan(shippable));
    }

    MonitoringPlanPtr MonitoringPlan::createDefaultPlan() {
        std::set<MetricCollectorType> metricCollectors{CPU_COLLECTOR,
                                                       DISK_COLLECTOR,
                                                       MEMORY_COLLECTOR,
                                                       NETWORK_COLLECTOR,
                                                       STATIC_SYSTEM_METRICS_COLLECTOR,
                                                       RUNTIME_METRICS_COLLECTOR};
        return MonitoringPlan::create(metricCollectors);
    }

    bool MonitoringPlan::addMetric(MetricCollectorType metric) {
        if (hasMetric(metric)) {
            return false;
        }
        metricTypes.insert(metric);
        return true;
    }

    bool MonitoringPlan::hasMetric(MetricCollectorType metric) const { return metricTypes.contains(metric); }

    SerializableMonitoringPlan MonitoringPlan::serialize() const {
        SerializableMonitoringPlan serPlan;
        for (auto mType : metricTypes) {
            serPlan.add_metrictypes(mType);
        }
        return serPlan;
    }

    std::string MonitoringPlan::toString() const {
        std::stringstream output;
        output << "MonitoringPlan:";

        for (auto metric : metricTypes) {
            switch (metric) {
                case MetricCollectorType::CPU_COLLECTOR: {
                    output << "cpu(True);";
                };
                case MetricCollectorType::DISK_COLLECTOR: {
                    output << "disk(True);";
                };
                case MetricCollectorType::MEMORY_COLLECTOR: {
                    output << "memory(True);";
                };
                case MetricCollectorType::NETWORK_COLLECTOR: {
                    output << "network(True);";
                };
                case MetricCollectorType::RUNTIME_METRICS_COLLECTOR: {
                    output << "runtimeMetrics(True);";
                };
                case MetricCollectorType::STATIC_SYSTEM_METRICS_COLLECTOR: {
                    output << "staticMetrics(True);";
                };
                default: {
                }
            }
        }
        return output.str();
    }

    std::ostream& operator<<(std::ostream& strm, const MonitoringPlan& plan) { return strm << plan.toString(); }

    const std::set<MetricCollectorType>& MonitoringPlan::getMetricTypes() const { return metricTypes; }

}// namespace NES
