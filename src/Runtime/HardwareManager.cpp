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

#include <Runtime/HardwareManager.hpp>
#include <Util/UtilityFunctions.hpp>

#ifdef __APPLE__
#include <sys/sysctl.h>
#include <sys/types.h>
#endif

#ifdef NES_ENABLE_NUMA_SUPPORT
#if defined(__linux__)
#include <numa.h>
#include <numaif.h>
#endif
#endif

namespace NES::Runtime {
namespace detail {

void readCpuConfig(uint32_t& numa_nodes_count,
                   uint32_t& num_physical_cpus,
                   std::vector<HardwareManager::NumaDescriptor>& cpuMapping) {
#ifdef __linux__
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
                Util::trim(s);
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
#elif defined(__APPLE__)
    int count;
    size_t len = sizeof(count);
    sysctlbyname("hw.physicalcpu", &count, &len, NULL, 0);
    numa_nodes_count = 1;
    num_physical_cpus = count;
    auto ref = cpuMapping.emplace_back(0);
    for (uint32_t i = 0; i < num_physical_cpus; ++i) {
        ref.addCpu(0, i);
    }
#else
#error "OS not supported"
#endif
}

}// namespace detail

HardwareManager::HardwareManager() : globalAllocator(std::make_shared<NesDefaultMemoryAllocator>()) {
    detail::readCpuConfig(numaNodesCount, numPhysicalCpus, cpuMapping);
#ifdef NES_ENABLE_NUMA_SUPPORT
    numaRegions.resize(numaNodesCount);
    for (uint32_t i = 0; i < numaNodesCount; ++i) {
        numaRegions[i] = std::make_shared<NumaRegionMemoryAllocator>(i);
    }
#endif
}
#ifdef NES_ENABLE_NUMA_SUPPORT
uint32_t HardwareManager::getNumberOfNumaRegions() const { return numaRegions.size(); }

NumaRegionMemoryAllocatorPtr HardwareManager::getNumaAllactor(uint32_t numaNodeIndex) const {
    NES_ASSERT2_FMT(numaNodeIndex < numaRegions.size(), "Invalid numa region " << numaNodeIndex);
    return numaRegions[numaNodeIndex];
}
#endif
NesDefaultMemoryAllocatorPtr HardwareManager::getGlobalAllocator() const { return globalAllocator; }


uint32_t HardwareManager::getMyNumaRegion() const {
#ifdef NES_ENABLE_NUMA_SUPPORT
    return numa_node_of_cpu(sched_getcpu());
#else
    return 0;
#endif
}

uint32_t HardwareManager::getNumaNodeForCore(int coreId) const {
#ifdef NES_ENABLE_NUMA_SUPPORT
    return numa_node_of_cpu(coreId);
#else
    return 0;
#endif
}

bool HardwareManager::bindThreadToCore(pthread_t, uint32_t, uint32_t) {
    NES_ASSERT2_FMT(false, "This will implemented at some point");
    return false;
}

}// namespace NES::Runtime