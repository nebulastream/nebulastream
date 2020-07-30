#include <Monitoring/Util/MetricUtils.hpp>

#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/CpuValues.hpp>
#include <Monitoring/MetricValues/DiscMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/Util/SystemResourcesReader.hpp>

namespace NES {

std::shared_ptr<Gauge<CpuMetrics>> MetricUtils::CPUStats() {
    return std::make_shared<Gauge<CpuMetrics>>(SystemResourcesReader::ReadCPUStats);
}

std::shared_ptr<Gauge<MemoryMetrics>> MetricUtils::MemoryStats() {
    return std::make_shared<Gauge<MemoryMetrics>>(SystemResourcesReader::ReadMemoryStats);
}

std::shared_ptr<Gauge<DiskMetrics>> MetricUtils::DiskStats() {
    return std::make_shared<Gauge<DiskMetrics>>(SystemResourcesReader::ReadDiskStats);
}

std::shared_ptr<Gauge<NetworkMetrics>> MetricUtils::NetworkStats() {
    return std::make_shared<Gauge<NetworkMetrics>>(SystemResourcesReader::ReadNetworkStats);
}

std::shared_ptr<Gauge<uint64_t>> MetricUtils::CPUIdle(unsigned int cpuNo) {
    auto gaugeIdle = std::make_shared<Gauge<uint64_t>>([cpuNo]() {
        NES_DEBUG("MetricUtils: Reading CPU Idle for cpu " << cpuNo);
        return SystemResourcesReader::ReadCPUStats().getValues(cpuNo).IDLE;
    });
    return gaugeIdle;
}

}// namespace NES