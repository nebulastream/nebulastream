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

#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>

namespace NES {

std::string toString(MetricCollectorType type) {
    switch (type) {
        case MetricCollectorType::CPU_COLLECTOR: return "cpu";
        case MetricCollectorType::DISK_COLLECTOR: return "disk";
        case MetricCollectorType::MEMORY_COLLECTOR: return "memory";
        case MetricCollectorType::NETWORK_COLLECTOR: return "network";
        case MetricCollectorType::STATIC_SYSTEM_METRICS_COLLECTOR: return "static";
        case MetricCollectorType::RUNTIME_METRICS_COLLECTOR: return "runtime";
        default: return "unknown";
    }
};

}// namespace NES