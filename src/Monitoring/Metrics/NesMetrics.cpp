#include <Monitoring/Metrics/NesMetrics.hpp>

#include <Monitoring/Util/SystemResourcesReader.hpp>

namespace NES {

Gauge<std::unordered_map<std::string, uint64_t>> NesMetrics::CPUStats() {
    return Gauge<std::unordered_map<std::string, uint64_t>>(SystemResourcesReader::CPUStats);
}

Gauge<std::unordered_map<std::string, uint64_t>> NesMetrics::MemoryStats() {
    return Gauge<std::unordered_map<std::string, uint64_t>>(SystemResourcesReader::MemoryStats);
}

Gauge<std::unordered_map<std::string, uint64_t>> NesMetrics::DiskStats() {
    return Gauge<std::unordered_map<std::string, uint64_t>>(SystemResourcesReader::DiskStats);
}

Gauge<uint64_t> NesMetrics::CPUIdle(std::string cpuName) {
    return Gauge<uint64_t>([&cpuName]() { return SystemResourcesReader::CPUStats()["idle" + cpuName]; });
}

Gauge<std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>>> NesMetrics::NetworkStats() {
    return Gauge<std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>>>(SystemResourcesReader::NetworkStats);
}

}