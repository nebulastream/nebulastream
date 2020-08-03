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