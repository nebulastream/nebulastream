#include <Monitoring/Util/SystemResourcesReader.hpp>
#include <Util/Logger.hpp>
#include <fstream>
#include <vector>
#include <iterator>
#include <sys/sysinfo.h>
#include <sys/statvfs.h>

namespace NES {

std::unordered_map<std::string, uint64_t> SystemResourcesReader::CPUStats() {
    std::unordered_map<std::string, uint64_t> output;

    std::ifstream fileStat("/proc/stat");
    std::string line;
    int cpuCount = 0;

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
            cpuCount++;

            char name[8];
            int len = tokens[0].copy(name, tokens[0].size());
            name[len] = '\0';

            output["user" + std::to_string(cpuCount)] = std::stoul(tokens[1]);
            output["nice" + std::to_string(cpuCount)] = std::stoul(tokens[2]);
            output["system" + std::to_string(cpuCount)] = std::stoul(tokens[3]);
            output["idle" + std::to_string(cpuCount)] = std::stoul(tokens[4]);
            output["iowait" + std::to_string(cpuCount)] = std::stoul(tokens[5]);
            output["irq" + std::to_string(cpuCount)] = std::stoul(tokens[6]);
            output["softIrq" + std::to_string(cpuCount)] = std::stoul(tokens[7]);
            output["steal" + std::to_string(cpuCount)] = std::stoul(tokens[8]);
            output["guest" + std::to_string(cpuCount)] = std::stoul(tokens[9]);
            output["guestNice" + std::to_string(cpuCount)] = std::stoul(tokens[10]);
        }
    }
    output["cpuCount"] = cpuCount - 1;
    NES_DEBUG("SystemResourcesReader: CPU stats read for number of CPUs " << output["cpuCount"]);
    return output;
}

std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> SystemResourcesReader::NetworkStats() {
    // alternatively also /sys/class/net/intf/statistics can be parsed
    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> output;

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
        output[ifname]["rBytes"] = rBytes;
        output[ifname]["rPackets"] = rPackets;
        output[ifname]["rErrs"] = rErrs;
        output[ifname]["rDrop"] = rDrop;
        output[ifname]["rFifo"] = rFifo;
        output[ifname]["rFrame"] = rFrame;
        output[ifname]["rCompressed"] = rCompressed;
        output[ifname]["rMulticast"] = rMulticast;

        output[ifname]["tBytes"] = tBytes;
        output[ifname]["tPackets"] = tPackets;
        output[ifname]["tErrs"] = tErrs;
        output[ifname]["tDrop"] = tDrop;
        output[ifname]["tFifo"] = tFifo;
        output[ifname]["tColls"] = tColls;
        output[ifname]["tCarrier"] = tCarrier;
        output[ifname]["tCompressed"] = tCompressed;
    }
    fclose(fp);

    return output;
}

std::unordered_map<std::string, uint64_t> SystemResourcesReader::MemoryStats() {
    auto* sinfo = (struct sysinfo*) malloc(sizeof(struct sysinfo));
    std::unordered_map<std::string, uint64_t> output;

    int ret = sysinfo(sinfo);
    if (ret == EFAULT)
        NES_THROW_RUNTIME_ERROR("SystemResourcesReader: Error reading memory stats");

    output["totalram"] = sinfo->totalram;
    output["totalswap"] = sinfo->totalswap;
    output["freeram"] = sinfo->freeram;
    output["sharedram"] = sinfo->sharedram;
    output["bufferram"] = sinfo->bufferram;
    output["freeswap"] = sinfo->freeswap;
    output["totalhigh"] = sinfo->totalhigh;
    output["freehigh"] = sinfo->freehigh;
    output["procs"] = sinfo->procs;
    output["mem_unit"] = sinfo->mem_unit;
    output["loads_1min"] = sinfo->loads[0];
    output["loads_5min"] = sinfo->loads[1];
    output["loads_15min"] = sinfo->loads[2];

    return output;
}

std::unordered_map<std::string, uint64_t> SystemResourcesReader::DiskStats() {
    std::unordered_map<std::string, uint64_t> output;
    auto* svfs = (struct statvfs*) malloc(sizeof(struct statvfs));

    int ret = statvfs("/", svfs);
    if (ret == EFAULT)
        NES_THROW_RUNTIME_ERROR("SystemResourcesReader: Error reading disk stats");

    output["f_bsize"] = svfs->f_bsize;
    output["f_frsize"] = svfs->f_frsize;
    output["f_blocks"] = svfs->f_blocks;
    output["f_bfree"] = svfs->f_bfree;
    output["f_bavail"] = svfs->f_bavail;

    return output;
}

}