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

#include <Monitoring/Util/MetricUtils.hpp>

#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/CpuValues.hpp>
#include <Monitoring/MetricValues/DiscMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/Util/SystemResourcesReader.hpp>

namespace NES {

Gauge<CpuMetrics> MetricUtils::CPUStats() {
    return Gauge<CpuMetrics>(SystemResourcesReader::ReadCPUStats);
}

Gauge<MemoryMetrics> MetricUtils::MemoryStats() {
    return Gauge<MemoryMetrics>(SystemResourcesReader::ReadMemoryStats);
}

Gauge<DiskMetrics> MetricUtils::DiskStats() {
    return Gauge<DiskMetrics>(SystemResourcesReader::ReadDiskStats);
}

Gauge<NetworkMetrics> MetricUtils::NetworkStats() {
    return Gauge<NetworkMetrics>(SystemResourcesReader::ReadNetworkStats);
}

Gauge<uint64_t> MetricUtils::CPUIdle(unsigned int cpuNo) {
    auto gaugeIdle = Gauge<uint64_t>([cpuNo]() {
        NES_DEBUG("MetricUtils: Reading CPU Idle for cpu " << cpuNo);
        return SystemResourcesReader::ReadCPUStats().getValues(cpuNo).IDLE;
    });
    return gaugeIdle;
}

}// namespace NES