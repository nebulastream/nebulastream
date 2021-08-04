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

#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/CpuValues.hpp>
#include <Monitoring/MetricValues/DiskMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/MetricValues/NetworkValues.hpp>
#include <Monitoring/MetricValues/RuntimeNesMetrics.hpp>
#include <Monitoring/MetricValues/StaticNesMetrics.hpp>
#include <Monitoring/Util/SystemResourcesReader.hpp>

#include <Util/Logger.hpp>
#include <chrono>
#include <fstream>
#include <iterator>
#ifdef __linux__
#include <sys/statvfs.h>
#include <sys/sysinfo.h>
#elif defined(__APPLE__)
#else
#error "Uknown platform"
#endif
#include <thread>
#include <vector>

namespace NES {

RuntimeNesMetrics SystemResourcesReader::readRuntimeNesMetrics() {
#ifdef __linux__
    RuntimeNesMetrics output{};
    output.wallTimeNs = SystemResourcesReader::getWallTimeInNs();

    std::vector<std::string> metricLocations{"/sys/fs/cgroup/memory/memory.usage_in_bytes",
                                             "/sys/fs/cgroup/cpuacct/cpuacct.stat",
                                             "/sys/fs/cgroup/blkio/blkio.throttle.io_service_bytes"};

    NES_DEBUG("SystemResourcesReader: Reading memory.usage_in_bytes for metrics");
    std::string line;
    int i = 0;
    std::ifstream memoryLoc(metricLocations[0]);
    std::string memoryStr((std::istreambuf_iterator<char>(memoryLoc)), std::istreambuf_iterator<char>());
    output.memoryUsageInBytes = std::stoull(memoryStr);

    NES_DEBUG("SystemResourcesReader: Reading cpuacct.stat for metrics");
    std::ifstream cpuStat(metricLocations[1]);
    while (std::getline(cpuStat, line)) {
        std::istringstream ss(line);
        std::vector<std::string> tokens{std::istream_iterator<std::string>{ss}, std::istream_iterator<std::string>{}};

        char name[2];
        int len = tokens[0].copy(name, tokens[0].size());
        name[len] = '\0';

        if (i == 0) {
            output.cpuLoadInJiffies = std::stoull(tokens[1]);
        } else if (i == 1) {
            output.cpuLoadInJiffies += std::stoull(tokens[1]);
        } else {
            break;
        }
        i++;
    }

    NES_DEBUG("SystemResourcesReader: Reading blkio.throttle.io_service_bytes for metrics");
    std::ifstream fileStat(metricLocations[2]);
    while (std::getline(fileStat, line)) {
        std::istringstream ss(line);
        std::vector<std::string> tokens{std::istream_iterator<std::string>{ss}, std::istream_iterator<std::string>{}};

        char name[3];
        int len = tokens[0].copy(name, tokens[0].size());
        name[len] = '\0';

        if (tokens[1] == "Read") {
            output.blkioBytesRead += std::stoul(tokens[2]);
        } else if (tokens[1] == "Write") {
            output.blkioBytesWritten += std::stoul(tokens[2]);
        }
    }

    return output;
#else
    return RuntimeNesMetrics();
#endif
}

StaticNesMetrics SystemResourcesReader::readStaticNesMetrics() {
    StaticNesMetrics output{false, false};
#ifdef __linux__

    std::vector<std::string> metricLocations{"/sys/fs/cgroup/memory/memory.limit_in_bytes",
                                             "/sys/fs/cgroup/cpuacct/cpu.cfs_period_us",
                                             "/sys/fs/cgroup/cpuacct/cpu.cfs_quota_us"};

    NES_DEBUG("SystemResourcesReader: Reading memory.usage_in_bytes for metrics");
    std::string memLine;
    std::ifstream memoryLoc(metricLocations[0]);
    std::string memoryStr((std::istreambuf_iterator<char>(memoryLoc)), std::istreambuf_iterator<char>());

    // check if a limit is set for the given cgroup, the smaller value is the available RAM
    uint64_t systemMem = SystemResourcesReader::readMemoryStats().TOTAL_RAM;
    uint64_t limitMem = std::stoull(memoryStr);
    output.totalMemoryBytes = std::min(limitMem, systemMem);

    // CPU metrics
    auto cpuStats = SystemResourcesReader::readCpuStats();
    auto totalStats = cpuStats.getTotal();
    output.totalCPUJiffies = totalStats.user + totalStats.system + totalStats.idle;
    output.cpuCoreNum = SystemResourcesReader::readCpuStats().getNumCores();

    std::string periodLine;
    std::ifstream periodLoc(metricLocations[1]);
    std::string periodStr((std::istreambuf_iterator<char>(periodLoc)), std::istreambuf_iterator<char>());
    output.cpuPeriodUS = std::stoll(periodStr);

    std::string quotaLine;
    std::ifstream quotaLoc(metricLocations[2]);
    std::string quotaStr((std::istreambuf_iterator<char>(quotaLoc)), std::istreambuf_iterator<char>());
    output.cpuQuotaUS = std::stoll(quotaStr);
#endif
    return output;
}

CpuMetrics SystemResourcesReader::readCpuStats() {
#ifdef __linux__
    std::ifstream fileStat("/proc/stat");
    std::string line;
    unsigned int numCPU = std::thread::hardware_concurrency();
    auto cpu = std::vector<CpuValues>(numCPU);
    CpuValues totalCpu{};

    auto i = 0;
    while (std::getline(fileStat, line)) {
        // line starts with "cpu"
        if (!line.compare(0, 3, "cpu")) {
            std::istringstream ss(line);
            std::vector<std::string> tokens{std::istream_iterator<std::string>{ss}, std::istream_iterator<std::string>{}};

            // check columns
            if (tokens.size() != 11) {
                NES_THROW_RUNTIME_ERROR("SystemResourcesReader: /proc/stat incorrect");
            }

            char name[8];
            int len = tokens[0].copy(name, tokens[0].size());
            name[len] = '\0';

            auto cpuStats = CpuValues();
            cpuStats.user = std::stoul(tokens[1]);
            cpuStats.nice = std::stoul(tokens[2]);
            cpuStats.system = std::stoul(tokens[3]);
            cpuStats.idle = std::stoul(tokens[4]);
            cpuStats.iowait = std::stoul(tokens[5]);
            cpuStats.irq = std::stoul(tokens[6]);
            cpuStats.softirq = std::stoul(tokens[7]);
            cpuStats.steal = std::stoul(tokens[8]);
            cpuStats.guest = std::stoul(tokens[9]);
            cpuStats.guestnice = std::stoul(tokens[10]);
            if (i == 0) {
                totalCpu = cpuStats;
            } else {
                cpu[i - 1] = cpuStats;
            }
            i++;
        }
    }
    NES_DEBUG("SystemResourcesReader: CPU stats read for number of CPUs " << i - 1);
    return CpuMetrics{totalCpu, numCPU, std::move(cpu)};
#else
    return CpuMetrics();
#endif
}

NetworkMetrics SystemResourcesReader::readNetworkStats() {
    auto output = NetworkMetrics();
#ifdef __linux__
    // alternatively also /sys/class/net/intf/statistics can be parsed


    FILE* fp = fopen("/proc/net/dev", "re");
    char buf[200];
    char ifname[20];
    uint64_t rBytes = 0;
    uint64_t rPackets = 0;
    uint64_t rErrs = 0;
    uint64_t rDrop = 0;
    uint64_t rFifo = 0;
    uint64_t rFrame = 0;
    uint64_t rCompressed = 0;
    uint64_t rMulticast = 0;
    uint64_t tBytes = 0;
    uint64_t tPackets = 0;
    uint64_t tErrs = 0;
    uint64_t tDrop = 0;
    uint64_t tFifo = 0;
    uint64_t tColls = 0;
    uint64_t tCarrier = 0;
    uint64_t tCompressed = 0;

    // skip first two lines
    for (int i = 0; i < 2; i++) {
        fgets(buf, 200, fp);
    }

    uint64_t i = 0;
    while (fgets(buf, 200, fp)) {
        sscanf(buf,
               "%[^:]: %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu",
               ifname,
               &rBytes,
               &rPackets,
               &rErrs,
               &rDrop,
               &rFifo,
               &rFrame,
               &rCompressed,
               &rMulticast,
               &tBytes,
               &tPackets,
               &tErrs,
               &tDrop,
               &tFifo,
               &tColls,
               &tCarrier,
               &tCompressed);
        auto outputValue = NetworkValues();
        // the name of the network interface
        // TODO: add proper handling of ifname as string (see issue #1725)
        outputValue.interfaceName = i++;

        // the received metric fields
        outputValue.rBytes = rBytes;
        outputValue.rPackets = rPackets;
        outputValue.rErrs = rErrs;
        outputValue.rDrop = rDrop;
        outputValue.rFifo = rFifo;
        outputValue.rFrame = rFrame;
        outputValue.rCompressed = rCompressed;
        outputValue.rMulticast = rMulticast;

        // the transmitted metric fields
        outputValue.tBytes = tBytes;
        outputValue.tPackets = tPackets;
        outputValue.tErrs = tErrs;
        outputValue.tDrop = tDrop;
        outputValue.tFifo = tFifo;
        outputValue.tColls = tColls;
        outputValue.tCarrier = tCarrier;
        outputValue.tCompressed = tCompressed;

        // extension of the wrapper class object
        output.addNetworkValues(std::move(outputValue));
    }
    fclose(fp);
#endif
    return output;
}

MemoryMetrics SystemResourcesReader::readMemoryStats() {
    auto output = MemoryMetrics();
#ifdef __linux__
    auto* sinfo = (struct sysinfo*) malloc(sizeof(struct sysinfo));

    int ret = sysinfo(sinfo);
    if (ret == EFAULT) {
        NES_THROW_RUNTIME_ERROR("SystemResourcesReader: Error reading memory stats");
    }

    output.TOTAL_RAM = sinfo->totalram;
    output.TOTAL_SWAP = sinfo->totalswap;
    output.FREE_RAM = sinfo->freeram;
    output.SHARED_RAM = sinfo->sharedram;
    output.BUFFER_RAM = sinfo->bufferram;
    output.FREE_SWAP = sinfo->freeswap;
    output.TOTAL_HIGH = sinfo->totalhigh;
    output.FREE_HIGH = sinfo->freehigh;
    output.PROCS = sinfo->procs;
    output.MEM_UNIT = sinfo->mem_unit;
    output.LOADS_1MIN = sinfo->loads[0];
    output.LOADS_5MIN = sinfo->loads[1];
    output.LOADS_15MIN = sinfo->loads[2];

    delete[] sinfo;
#endif
    return output;
}

DiskMetrics SystemResourcesReader::readDiskStats() {
    DiskMetrics output{};
#ifdef __linux__
    auto* svfs = (struct statvfs*) malloc(sizeof(struct statvfs));

    int ret = statvfs("/", svfs);
    if (ret == EFAULT) {
        NES_THROW_RUNTIME_ERROR("SystemResourcesReader: Error reading disk stats");
    }

    output.fBsize = svfs->f_bsize;
    output.fFrsize = svfs->f_frsize;
    output.fBlocks = svfs->f_blocks;
    output.fBfree = svfs->f_bfree;
    output.fBavail = svfs->f_bavail;
#endif
    return output;
}

uint64_t SystemResourcesReader::getWallTimeInNs() {
    auto now = std::chrono::system_clock::now();
    auto now_s = std::chrono::time_point_cast<std::chrono::nanoseconds>(now);
    auto epoch = now_s.time_since_epoch();
    auto value = std::chrono::duration_cast<std::chrono::nanoseconds>(epoch);
    uint64_t duration = value.count();
    return duration;
}

}// namespace NES