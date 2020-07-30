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
    auto cpu = std::unique_ptr<CpuValues[]>(new CpuValues[numCPU]);
    CpuValues totalCpu{};

    int i = 0;
    while (std::getline(fileStat, line)) {
        // line starts with "cpu"
        if (!line.compare(0, 3, "cpu")) {
            std::istringstream ss(line);
            std::vector<std::string> tokens{
                std::istream_iterator<std::string>{ss},
                std::istream_iterator<std::string>{}};

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
        sscanf(buf, "%[^:]: %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu",
               ifname, &rBytes, &rPackets, &rErrs, &rDrop, &rFifo, &rFrame, &rCompressed, &rMulticast,
               &tBytes, &tPackets, &tErrs, &tDrop, &tFifo, &tColls, &tCarrier, &tCompressed);
        output[ifname].rBytes = rBytes;
        output[ifname].rPackets = rPackets;
        output[ifname].rErrs = rErrs;
        output[ifname].rDrop = rDrop;
        output[ifname].rFifo = rFifo;
        output[ifname].rFrame = rFrame;
        output[ifname].rCompressed = rCompressed;
        output[ifname].rMulticast = rMulticast;

        output[ifname].tBytes = tBytes;
        output[ifname].tPackets = tPackets;
        output[ifname].tErrs = tErrs;
        output[ifname].tDrop = tDrop;
        output[ifname].tFifo = tFifo;
        output[ifname].tColls = tColls;
        output[ifname].tCarrier = tCarrier;
        output[ifname].tCompressed = tCompressed;
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

    return output;
}

DiskMetrics SystemResourcesReader::ReadDiskStats() {
    DiskMetrics output;
    auto* svfs = (struct statvfs*) malloc(sizeof(struct statvfs));

    int ret = statvfs("/", svfs);
    if (ret == EFAULT)
        NES_THROW_RUNTIME_ERROR("SystemResourcesReader: Error reading disk stats");

    output.F_BSIZE = svfs->f_bsize;
    output.F_FRSIZE = svfs->f_frsize;
    output.F_BLOCKS = svfs->f_blocks;
    output.F_BFREE = svfs->f_bfree;
    output.F_BAVAIL = svfs->f_bavail;

    return output;
}

}// namespace NES