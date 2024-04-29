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

#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <Monitoring/Metrics/Gauge/RuntimeMetrics.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Metrics/Wrapper/NetworkMetricsWrapper.hpp>
#include <Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp>

namespace NES::Monitoring {
AbstractSystemResourcesReader::AbstractSystemResourcesReader() : readerType(SystemResourcesReaderType::AbstractReader) {}

RuntimeMetrics AbstractSystemResourcesReader::readRuntimeNesMetrics() {
    RuntimeMetrics output{};
    return output;
}

RegistrationMetrics AbstractSystemResourcesReader::readRegistrationMetrics() {
    RegistrationMetrics output;
    return output;
}

CpuMetricsWrapper AbstractSystemResourcesReader::readCpuStats() {
    std::vector<CpuMetrics> arr{CpuMetrics()};
    CpuMetricsWrapper output{std::move(arr)};
    return output;
}

NetworkMetricsWrapper AbstractSystemResourcesReader::readNetworkStats() {
    std::vector<NetworkMetrics> arr{NetworkMetrics()};
    NetworkMetricsWrapper output{std::move(arr)};
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
}// namespace NES::Monitoring
