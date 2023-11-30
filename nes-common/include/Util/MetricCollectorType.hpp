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

#ifndef NES_COMMON_INCLUDE_UTIL_METRICCOLLECTORTYPE_HPP_
#define NES_COMMON_INCLUDE_UTIL_METRICCOLLECTORTYPE_HPP_

#include <cstdint>
#include <string>

namespace NES::Monitoring {
enum class MetricCollectorType : uint8_t {
    CPU_COLLECTOR,
    DISK_COLLECTOR,
    MEMORY_COLLECTOR,
    NETWORK_COLLECTOR,
    STATIC_SYSTEM_METRICS_COLLECTOR,
    RUNTIME_METRICS_COLLECTOR,
    INVALID
};

}// namespace NES::Monitoring

#endif  // NES_COMMON_INCLUDE_UTIL_METRICCOLLECTORTYPE_HPP_
