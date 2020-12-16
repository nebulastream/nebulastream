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
#include <Monitoring/MetricValues/DiscMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/MetricValues/NetworkValues.hpp>
#include <Monitoring/Util/SystemResourcesReader.hpp>

#include <Util/Logger.hpp>
#include <fstream>
#include <iterator>
#include <sys/statvfs.h>
#include <sys/sysinfo.h>
#include <thread>
#include <vector>

namespace NES {

CpuMetrics SystemResourcesReader::ReadCPUStats() {
    std::ifstream fileStat("/proc/stat");
    std::string line;
    unsigned int numCPU = std::thread::hardware_concurrency();
    auto cpu = std::vector<CpuValues>(numCPU);
    CpuValues totalCpu{};

    int i = 0;
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
            cpuStats.USER = std::stoul(tokens[1]);
            cpuStats.NICE = std::stoul(tokens[2]);
            cpuStats.SYSTEM = std::stoul(tokens[3]);
            cpuStats.IDLE = std::stoul(tokens[4]);
            cpuStats.IOWAIT = std::stoul(tokens[5]);
            cpuStats.IRQ = std::stoul(tokens[6]);
            cpuStats.SOFTIRQ = std::stoul(tokens[7]);
            cpuStats.STEAL = std::stoul(tokens[8]);
            cpuStats.GUEST = std::stoul(tokens[9]);
            cpuStats.GUESTNICE = std::stoul(tokens[10]);
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
}

NetworkMetrics SystemResourcesReader::ReadNetworkStats() {
    // alternatively also /sys/class/net/intf/statistics can be parsed
    auto output = NetworkMetrics();

    FILE* fp = fopen("/proc/net/dev", "r");
    char buf[200], ifname[20];
    uint64_t rBytes, rPackets, rErrs, rDrop, rFifo, rFrame, rCompressed, rMulticast;
    uint64_t tBytes, tPackets, tErrs, tDrop, tFifo, tColls, tCarrier, tCompressed;

    // skip first two lines
    for (int i = 0; i < 2; i++) {
        fgets(buf, 200, fp);
    }

    while (fgets(buf, 200, fp)) {
        sscanf(buf, "%[^:]: %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu", ifname, &rBytes, &rPackets, &rErrs,
               &rDrop, &rFifo, &rFrame, &rCompressed, &rMulticast, &tBytes, &tPackets, &tErrs, &tDrop, &tFifo, &tColls, &tCarrier,
               &tCompressed);
        auto outputValue = NetworkValues();
        // the name of the network interface
        outputValue.interfaceName = ifname;

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

    return output;
}

MemoryMetrics SystemResourcesReader::ReadMemoryStats() {
    auto* sinfo = (struct sysinfo*) malloc(sizeof(struct sysinfo));

    int ret = sysinfo(sinfo);
    if (ret == EFAULT)
        NES_THROW_RUNTIME_ERROR("SystemResourcesReader: Error reading memory stats");

    MemoryMetrics output{};
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
    return output;
}

DiskMetrics SystemResourcesReader::ReadDiskStats() {
    DiskMetrics output;
    auto* svfs = (struct statvfs*) malloc(sizeof(struct statvfs));

    int ret = statvfs("/", svfs);
    if (ret == EFAULT)
        NES_THROW_RUNTIME_ERROR("SystemResourcesReader: Error reading disk stats");

    output.fBsize = svfs->f_bsize;
    output.fFrsize = svfs->f_frsize;
    output.fBlocks = svfs->f_blocks;
    output.fBfree = svfs->f_bfree;
    output.fBavail = svfs->f_bavail;

    return output;
}

}// namespace NES