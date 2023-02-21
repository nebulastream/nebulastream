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

#include <Monitoring/Metrics/MetricType.hpp>

namespace NES::Monitoring {

std::string toString(MetricType type) {
    switch (type) {
        case MetricType::CpuMetric: return "cpu";
        case MetricType::DiskMetric: return "disk";
        case MetricType::MemoryMetric: return "memory";
        case MetricType::NetworkMetric: return "network";
        case MetricType::RuntimeMetric: return "runtime";
        case MetricType::RegistrationMetric: return "registration";
        case MetricType::WrappedCpuMetrics: return "wrapped_cpu";
        case MetricType::WrappedNetworkMetrics: return "wrapped_network";
        default: return "unknown";
    }
};

MetricType parse(std::string metricTypeString) {
    if (metricTypeString.find("wrapped_cpu") != std::string::npos) {
        return MetricType::WrappedCpuMetrics;
    } else if (metricTypeString.find("disk") != std::string::npos) {
        return MetricType::DiskMetric;
    } else if (metricTypeString.find("memory") != std::string::npos) {
        return MetricType::MemoryMetric;
    } else if (metricTypeString.find("wrapped_network") != std::string::npos) {
        return MetricType::WrappedNetworkMetrics;
    } else if (metricTypeString.find("runtime") != std::string::npos) {
        return MetricType::RuntimeMetric;
    } else if (metricTypeString.find("registration") != std::string::npos) {
        return MetricType::RegistrationMetric;
    } else if (metricTypeString.find("network") != std::string::npos) {
        return MetricType::NetworkMetric;
    } else if (metricTypeString.find("cpu") != std::string::npos) {
        return MetricType::CpuMetric;
    }
    return MetricType::UnknownMetric;
}

}// namespace NES::Monitoring