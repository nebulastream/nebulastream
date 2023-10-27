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

#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Monitoring {
MonitoringPlan::MonitoringPlan(const std::set<MetricType>& metrics) : metricTypes(metrics) {
    NES_DEBUG("MonitoringPlan: Init with metrics of size {}", metrics.size());
}

MonitoringPlanPtr MonitoringPlan::create(const std::set<MetricType>& metrics) {
    return std::shared_ptr<MonitoringPlan>(new MonitoringPlan(metrics));
}

MonitoringPlanPtr MonitoringPlan::defaultPlan() {
    std::set<MetricType> metricTypes{MetricType::WrappedCpuMetrics,
                                     MetricType::DiskMetric,
                                     MetricType::MemoryMetric,
                                     MetricType::WrappedNetworkMetrics};
    return MonitoringPlan::create(metricTypes);
}

std::set<MetricCollectorType> MonitoringPlan::defaultCollectors() {
    return std::set<MetricCollectorType>{MetricCollectorType::CPU_COLLECTOR,
                                         MetricCollectorType::DISK_COLLECTOR,
                                         MetricCollectorType::MEMORY_COLLECTOR,
                                         MetricCollectorType::NETWORK_COLLECTOR};
}

bool MonitoringPlan::addMetric(MetricType metric) {
    if (hasMetric(metric)) {
        return false;
    }
    metricTypes.insert(metric);
    return true;
}

bool MonitoringPlan::hasMetric(MetricType metric) const { return metricTypes.contains(metric); }

std::string MonitoringPlan::toString() const {
    std::stringstream output;
    output << "MonitoringPlan:";

    for (auto metric : metricTypes) {
        switch (metric) {
            case MetricType::CpuMetric: {
                output << "cpu(True);";
            };
            case MetricType::DiskMetric: {
                output << "disk(True);";
            };
            case MetricType::MemoryMetric: {
                output << "memory(True);";
            };
            case MetricType::NetworkMetric: {
                output << "network(True);";
            };
            case MetricType::RuntimeMetric: {
                output << "runtimeMetrics(True);";
            };
            case MetricType::RegistrationMetric: {
                output << "staticMetrics(True);";
            };
            default: {
            }
        }
    }
    return output.str();
}

std::ostream& operator<<(std::ostream& strm, const MonitoringPlan& plan) { return strm << plan.toString(); }

const std::set<MetricType>& MonitoringPlan::getMetricTypes() const { return metricTypes; }

const std::set<MetricCollectorType> MonitoringPlan::getCollectorTypes() const {
    std::set<MetricCollectorType> output;
    for (auto mType : getMetricTypes()) {
        output.insert(MetricUtils::createCollectorTypeFromMetricType(mType));
    }
    return output;
}

}// namespace NES::Monitoring
