#include <Monitoring/Util/MetricUtils.hpp>

#include <Monitoring/Util/SystemResourcesReader.hpp>
#include <Monitoring/MetricValues/CPU.hpp>
#include <Monitoring/MetricValues/CpuStats.hpp>

namespace NES {

Gauge<CPU> MetricUtils::CPUStats() {
    return Gauge<CPU>(SystemResourcesReader::ReadCPUStats);
}

Gauge<std::unordered_map<std::string, uint64_t>> MetricUtils::MemoryStats() {
    return Gauge<std::unordered_map<std::string, uint64_t>>(SystemResourcesReader::MemoryStats);
}

Gauge<std::unordered_map<std::string, uint64_t>> MetricUtils::DiskStats() {
    return Gauge<std::unordered_map<std::string, uint64_t>>(SystemResourcesReader::DiskStats);
}

Gauge<uint64_t> MetricUtils::CPUIdle(unsigned int cpuNo) {
    return Gauge<uint64_t>([cpuNo]() {
        NES_DEBUG("MetricUtils: Reading CPU Idle for cpu " << cpuNo);
        return SystemResourcesReader::ReadCPUStats()[cpuNo].IDLE;
    });
}

Gauge<std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>>> MetricUtils::NetworkStats() {
    return Gauge<std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>>>(SystemResourcesReader::NetworkStats);
}

}