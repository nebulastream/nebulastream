/*
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

#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Gauge/NetworkMetrics.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <Monitoring/Metrics/Gauge/RuntimeMetrics.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Metrics/Wrapper/NetworkMetricsWrapper.hpp>
#include <Monitoring/ResourcesReader/LinuxSystemResourcesReader.hpp>
#include <Util/Logger/Logger.hpp>
#include <chrono>
#include <fstream>
#include <iterator>

#include <string>
#include <sys/statvfs.h>
#include <sys/sysinfo.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace NES::Monitoring {
LinuxSystemResourcesReader::LinuxSystemResourcesReader() { readerType = SystemResourcesReaderType::LinuxReader; }

RuntimeMetrics LinuxSystemResourcesReader::readRuntimeNesMetrics() {
    RuntimeMetrics output{};
    output.wallTimeNs = LinuxSystemResourcesReader::getWallTimeInNs();

    std::vector<std::string> metricLocations{"/sys/fs/cgroup/memory/memory.usage_in_bytes",
                                             "/sys/fs/cgroup/cpuacct/cpuacct.stat",
                                             "/sys/fs/cgroup/blkio/blkio.throttle.io_service_bytes"};

    try {
        NES_TRACE("LinuxSystemResourcesReader: Reading memory.usage_in_bytes for metrics");

        if (access(metricLocations[0].c_str(), F_OK) != -1) {
            std::ifstream memoryLoc(metricLocations[0]);
            std::string memoryStr((std::istreambuf_iterator<char>(memoryLoc)), std::istreambuf_iterator<char>());

            output.memoryUsageInBytes = std::stoull(memoryStr);
        } else {
            NES_ERROR("LinuxSystemResourcesReader: File for memory.usage_in_bytes not available");
            output.memoryUsageInBytes = 0;
        }
    } catch (const Exceptions::RuntimeException& e) {
        NES_ERROR("LinuxSystemResourcesReader: Error reading memory metrics {}", e.what());
    }

    try {
        NES_TRACE("LinuxSystemResourcesReader: Reading cpuacct.stat for metrics");

        if (access(metricLocations[1].c_str(), F_OK) != -1) {
            std::string line;
            int i = 0;

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
        } else {
            NES_ERROR("LinuxSystemResourcesReader: File for cpuacct.stat not available");
            output.cpuLoadInJiffies = 0;
        }
    } catch (const Exceptions::RuntimeException& e) {
        NES_ERROR("LinuxSystemResourcesReader: Error reading cpu metrics {}", e.what());
    }

    try {
        NES_TRACE("LinuxSystemResourcesReader: Reading blkio.throttle.io_service_bytes for metrics");

        if (access(metricLocations[2].c_str(), F_OK) != -1) {
            std::string line;

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
        } else {
            NES_ERROR("LinuxSystemResourcesReader: File for blkio.throttle.io_service_bytes not available");
            output.blkioBytesRead = 0;
            output.blkioBytesWritten = 0;
        }

    } catch (const Exceptions::RuntimeException& e) {
        NES_ERROR("LinuxSystemResourcesReader: Error reading disk metrics {}", e.what());
    }

    return output;
}

RegistrationMetrics LinuxSystemResourcesReader::readRegistrationMetrics() {
    RegistrationMetrics output;

    std::vector<std::string> metricLocations{"/sys/fs/cgroup/memory/memory.limit_in_bytes",
                                             "/sys/fs/cgroup/cpuacct/cpu.cfs_period_us",
                                             "/sys/fs/cgroup/cpuacct/cpu.cfs_quota_us"};

    // memory metrics
    try {
        NES_TRACE("LinuxSystemResourcesReader: Reading memory.usage_in_bytes for metrics");
        uint64_t systemMem = LinuxSystemResourcesReader::readMemoryStats().TOTAL_RAM;
        output.totalMemoryBytes = systemMem;
    } catch (const Exceptions::RuntimeException& e) {
        NES_ERROR("LinuxSystemResourcesReader: Error reading static memory metrics {}", e.what());
    }

    // CPU metrics
    try {
        auto cpuStats = LinuxSystemResourcesReader::readCpuStats();
        auto totalStats = cpuStats.getTotal();
        output.totalCPUJiffies = totalStats.user + totalStats.system + totalStats.idle;
        output.cpuCoreNum = LinuxSystemResourcesReader::readCpuStats().size() - 1;

        if (access(metricLocations[1].c_str(), F_OK) != -1) {
            std::string periodLine;
            std::ifstream periodLoc(metricLocations[1]);
            std::string periodStr((std::istreambuf_iterator<char>(periodLoc)), std::istreambuf_iterator<char>());

            output.cpuPeriodUS = std::stoll(periodStr);
        } else {
            NES_WARNING("LinuxSystemResourcesReader: File for cpu.cfs_period_us not available");
            output.cpuPeriodUS = 0;
        }

        if (access(metricLocations[2].c_str(), F_OK) != -1) {
            std::string quotaLine;
            std::ifstream quotaLoc(metricLocations[2]);
            std::string quotaStr((std::istreambuf_iterator<char>(quotaLoc)), std::istreambuf_iterator<char>());

            output.cpuQuotaUS = std::stoll(quotaStr);
        } else {
            NES_WARNING("LinuxSystemResourcesReader: File for cpu.cfs_quota_us not available");
            output.cpuQuotaUS = 0;
        }
    } catch (const Exceptions::RuntimeException& e) {
        NES_ERROR("LinuxSystemResourcesReader: Error reading static cpu metrics {}", e.what());
    }
    return output;
}

CpuMetricsWrapper LinuxSystemResourcesReader::readCpuStats() {
    std::string metricLocation = "/proc/stat";
    unsigned int numCpuMetrics = std::thread::hardware_concurrency() + 1;
    auto cpu = std::vector<CpuMetrics>(numCpuMetrics);

    if (access(metricLocation.c_str(), F_OK) == -1) {
        return AbstractSystemResourcesReader::readCpuStats();
    }

    try {
        NES_TRACE("LinuxSystemResourcesReader: Reading CPU stats for number of CPUs {}", numCpuMetrics);
        std::ifstream fileStat(metricLocation);
        std::string line;

        auto i = 0;
        while (std::getline(fileStat, line)) {
            // line starts with "cpu"
            if (!line.compare(0, 3, "cpu")) {
                std::istringstream ss(line);
                std::vector<std::string> tokens{std::istream_iterator<std::string>{ss}, std::istream_iterator<std::string>{}};

                // check columns
                if (tokens.size() != 11) {
                    NES_THROW_RUNTIME_ERROR("LinuxSystemResourcesReader: /proc/stat incorrect");
                }

                char name[8];
                int len = tokens[0].copy(name, tokens[0].size());
                name[len] = '\0';

                auto cpuStats = CpuMetrics();
                cpuStats.coreNum = i;
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

                cpu[i] = cpuStats;
                i++;
            }
        }
    } catch (const Exceptions::RuntimeException& e) {
        NES_ERROR("LinuxSystemResourcesReader: Error calling readCpuStats() {}", e.what());
    }
    return CpuMetricsWrapper{std::move(cpu)};
}

NetworkMetricsWrapper LinuxSystemResourcesReader::readNetworkStats() {
    std::string metricLocation = "/proc/net/dev";
    auto output = NetworkMetricsWrapper();

    if (access(metricLocation.c_str(), F_OK) == -1) {
        return AbstractSystemResourcesReader::readNetworkStats();
    }

    try {
        NES_TRACE("LinuxSystemResourcesReader: Reading network stats.");

        // alternatively also /sys/class/net/intf/statistics can be parsed
        FILE* fp = fopen(metricLocation.c_str(), "re");
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
            auto outputValue = NetworkMetrics();
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
            output.addNetworkMetrics(std::move(outputValue));
        }
        fclose(fp);

    } catch (const Exceptions::RuntimeException& e) {
        NES_ERROR("LinuxSystemResourcesReader: Error reading network stats {}", e.what());
    }
    return output;
}

MemoryMetrics LinuxSystemResourcesReader::readMemoryStats() {
    auto output = MemoryMetrics();

    try {
        NES_TRACE("LinuxSystemResourcesReader: Reading memory stats.");

        auto* sinfo = (struct sysinfo*) malloc(sizeof(struct sysinfo));

        int ret = sysinfo(sinfo);
        if (ret == EFAULT) {
            NES_THROW_RUNTIME_ERROR("LinuxSystemResourcesReader: Error reading memory stats");
        }

        output.timestamp = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
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
        free(sinfo);
    } catch (const Exceptions::RuntimeException& e) {
        NES_ERROR("LinuxSystemResourcesReader: Error reading memory stats {}", e.what());
    }
    return output;
}

DiskMetrics LinuxSystemResourcesReader::readDiskStats() {
    DiskMetrics output{};

    try {
        NES_TRACE("LinuxSystemResourcesReader: Reading disk stats.");
        auto* svfs = (struct statvfs*) malloc(sizeof(struct statvfs));

        int ret = statvfs("/", svfs);
        if (ret == EFAULT) {
            NES_THROW_RUNTIME_ERROR("LinuxSystemResourcesReader: Error reading disk stats");
        }

        output.timestamp = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        output.fBsize = svfs->f_bsize;
        output.fFrsize = svfs->f_frsize;
        output.fBlocks = svfs->f_blocks;
        output.fBfree = svfs->f_bfree;
        output.fBavail = svfs->f_bavail;
        free(svfs);
    } catch (const Exceptions::RuntimeException& e) {
        NES_ERROR("LinuxSystemResourcesReader: Error reading disk stats {}", e.what());
    }
    return output;
}

uint64_t LinuxSystemResourcesReader::getWallTimeInNs() {
    auto now = std::chrono::system_clock::now();
    auto now_s = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto epoch = now_s.time_since_epoch();
    auto value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
    uint64_t duration = value.count();
    return duration;
}
}// namespace NES::Monitoring