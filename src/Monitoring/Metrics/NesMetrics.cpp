#include <Monitoring/Metrics/NesMetrics.hpp>

#include <Monitoring/Util/SystemResourcesReader.hpp>

namespace NES {

Gauge<std::unordered_map<std::string, uint64_t>> NesMetrics::CPUStats() {
    return Gauge<std::unordered_map<std::string, uint64_t>>(SystemResourcesReader::CPUStats);
}

Gauge<uint64_t> NesMetrics::CPUIdle(std::string cpuName) {
    return Gauge<uint64_t>([&cpuName]() { return SystemResourcesReader::CPUStats()["idle" + cpuName]; });
}

}