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

#include "Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp"
#include "Monitoring/Metrics/Gauge/CpuMetricsWrapper.hpp"
#include "Monitoring/Metrics/Gauge/DiskMetrics.hpp"
#include "Monitoring/Metrics/Gauge/MemoryMetrics.hpp"
#include "Monitoring/Metrics/Gauge/NetworkMetricsWrapper.hpp"
#include "Monitoring/Metrics/Gauge/RuntimeNesMetrics.hpp"
#include "Monitoring/Metrics/Gauge/StaticNesMetrics.hpp"

namespace NES {
AbstractSystemResourcesReader::AbstractSystemResourcesReader(): readerType(AbstractReader) {
}

RuntimeNesMetrics AbstractSystemResourcesReader::readRuntimeNesMetrics() {
    RuntimeNesMetrics output{};
    return output;
}

StaticNesMetrics AbstractSystemResourcesReader::readStaticNesMetrics() {
    StaticNesMetrics output{};
    return output;
}

CpuMetricsWrapper AbstractSystemResourcesReader::readCpuStats() {
    CpuMetricsWrapper output{};
    return output;
}

NetworkMetricsWrapper AbstractSystemResourcesReader::readNetworkStats() {
    NetworkMetricsWrapper output{};
    return output;
}

MemoryMetrics AbstractSystemResourcesReader::readMemoryStats() {
    MemoryMetrics output{};
    return output;
}

DiskMetrics AbstractSystemResourcesReader::readDiskStats() {
    DiskMetrics output{};
    return output;
}

uint64_t AbstractSystemResourcesReader::getWallTimeInNs() {
    uint64_t output{};
    return output;
}

SystemResourcesReaderType AbstractSystemResourcesReader::getReaderType() const { return readerType; }
}// namespace NES