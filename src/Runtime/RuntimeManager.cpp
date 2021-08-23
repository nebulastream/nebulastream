
#include <Runtime/RuntimeManager.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES::Runtime {
namespace detail {

void readCpuConfig(uint32_t& numa_nodes_count,
                   uint32_t& num_physical_cpus,
                   std::vector<RuntimeManager::NumaDescriptor>& cpuMapping) {
    std::stringstream buffer;
    FILE* fp = popen("lscpu --all --extended", "r");
    NES_ASSERT2_FMT(fp != nullptr, "Cannot execute `lscpu --all --extended`");
    char temp[1024];
    while (fgets(temp, sizeof(temp), fp) != nullptr) {
        buffer << temp;
    }
    NES_ASSERT2_FMT(pclose(fp) != -1, "Cannot close `lscpu --all --extended`");

    std::string line;
    while (std::getline(buffer, line)) {
        if (line.starts_with("CPU")) {
            continue;// skip first line
        }
        std::vector<std::string> splits;
        std::string s;
        std::istringstream lineStream(line);
        while (std::getline(lineStream, s, ' ')) {
            if (!s.empty()) {
                UtilityFunctions::trim(s);
                splits.push_back(s);
            }
        }
        auto online = splits[5] == "yes";
        if (!online) {
            continue;
        }
        unsigned long cpu = std::stoi(splits[0]);
        unsigned long node = std::stoi(splits[1]);
        unsigned long core = std::stoi(splits[3]);
        if (cpuMapping.empty() || cpuMapping.size() <= node) {
            cpuMapping.emplace_back(node);
        }
        auto& descriptor = cpuMapping[node];
        descriptor.addCpu(cpu, core);
        if (online) {
            numa_nodes_count = std::max<uint32_t>(numa_nodes_count, node + 1);
            num_physical_cpus = std::max<uint32_t>(num_physical_cpus, core + 1);
        }
    }
}

}// namespace detail

RuntimeManager::RuntimeManager() : globalAllocator(std::make_shared<NesDefaultMemoryAllocator>()) {
    detail::readCpuConfig(numaNodesCount, numPhysicalCpus, cpuMapping);
    numaRegions.resize(numaNodesCount);
    for (uint32_t i = 0; i < numaNodesCount; ++i) {
        numaRegions[i] = std::make_shared<NumaRegionMemoryAllocator>(i);
    }
}

uint32_t RuntimeManager::getNumberOfNumaRegions() const { return numaRegions.size(); }

NumaRegionMemoryAllocatorPtr RuntimeManager::getNumaAllactor(uint32_t numaNodeIndex) const {
    NES_ASSERT2_FMT(numaNodeIndex < numaRegions.size(), "Invalid numa region " << numaNodeIndex);
    return numaRegions[numaNodeIndex];
}

NesDefaultMemoryAllocatorPtr RuntimeManager::getGlobalAllocator() const {
    return globalAllocator;
}

}// namespace NES::Runtime