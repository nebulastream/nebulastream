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
#include <Monitoring/ResourcesReader/MacOsSystemResourceReader.hpp>
#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Gauge/NetworkMetrics.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <Monitoring/Metrics/Gauge/RuntimeMetrics.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Metrics/Wrapper/NetworkMetricsWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <chrono>
#include <fstream>
#include <iterator>
#include <log4cxx/helpers/exception.h>
#include <string>
#include <sys/statvfs.h>
//#include <sys/sysinfo.h>
#include <thread>
//#include <unistd.h>
//#include <unordered_map>
#include <sys/sysctl.h>
//#include <sys/types.h>
//#include <sys/socket.h>
#include <mach/mach.h>
//#include <mach/memory.h>
//#include <mach/processor_info.h>
//#include <mach/task_info.h>
//#include <mach/host_info.h>
//#include <mach/mach_host.h>
//#include <netinet/in.h>
#include <net/if.h>
#include <net/route.h>
#include <net/if_dl.h>



namespace NES {
MacOsSystemResourcesReader::MacOsSystemResourcesReader() { readerType = SystemResourcesReaderType::MacOsReader; }

RuntimeMetrics MacOsSystemResourcesReader::readRuntimeNesMetrics() {
    RuntimeMetrics output{};
    output.wallTimeNs = MacOsSystemResourcesReader::getWallTimeInNs();

    try {
        NES_DEBUG("MacOsSystemResourcesReader: Reading memoryUsageInBytes from task_info for metrics");

        mach_msg_type_number_t task_info_out_count = TASK_BASIC_INFO_COUNT;
        struct mach_task_basic_info task_inf;

        if (KERN_SUCCESS == task_info(mach_task_self(), TASK_BASIC_INFO, (task_info_t)&task_inf, &task_info_out_count)) {
            output.memoryUsageInBytes = task_inf.resident_size + task_inf.virtual_size;
        } else {
            NES_ERROR("MacOsSystemResourcesReader: task_info failed");
            output.memoryUsageInBytes = 0;
        }
    } catch (const log4cxx::helpers::RuntimeException& e) {
        NES_ERROR("MacOsSystemResourcesReader: Error reading memory metrics " << e.what());
    }

    try {
        NES_DEBUG("MacOsSystemResourcesReader: Reading cpuLoadInJiffies from host_processor_info for metrics");

        natural_t cpu_load_count;
        processor_cpu_load_info_t cpu_load_info;
        mach_msg_type_number_t cpu_load_info_count;

        if (KERN_SUCCESS == host_processor_info(mach_host_self(), PROCESSOR_CPU_LOAD_INFO, &cpu_load_count, (processor_info_array_t *)&cpu_load_info, &cpu_load_info_count)){
            uint64_t system = 0;
            uint64_t user = 0;

            for(natural_t i = 0; i < cpu_load_count; i++){
                system += cpu_load_info[i].cpu_ticks[CPU_STATE_SYSTEM];
                user += cpu_load_info[i].cpu_ticks[CPU_STATE_USER];
            }
            output.cpuLoadInJiffies = system + user;
        } else {
            NES_ERROR("MacOsSystemResourcesReader: host_processor_info failed");
            output.cpuLoadInJiffies = 0;
}
    } catch (const log4cxx::helpers::RuntimeException& e) {
        NES_ERROR("MacOsSystemResourcesReader: Error reading cpu metrics " << e.what());
    }

    try {
        NES_DEBUG("MacOsSystemResourcesReader: Reading blkioBytes for metrics");

        //TODO: bytes transferred by disk
        NES_ERROR("MacOsSystemResourcesReader: blkioBytes not available");
        output.blkioBytesRead = 0;
        output.blkioBytesWritten = 0;

    } catch (const log4cxx::helpers::RuntimeException& e) {
        NES_ERROR("MacOsSystemResourcesReader: Error reading disk metrics " << e.what());
    }

    return output;

}

RegistrationMetrics MacOsSystemResourcesReader::readRegistrationMetrics() {
    RegistrationMetrics output{false, false};

    // memory metrics
    try {
        NES_DEBUG("MacOsSystemResourcesReader: Reading memory.usage_in_bytes for metrics");

        output.totalMemoryBytes = MacOsSystemResourcesReader::readMemoryStats().TOTAL_RAM;
    } catch (const log4cxx::helpers::RuntimeException& e) {
        NES_ERROR("MacOsSystemResourcesReader: Error reading static memory metrics " << e.what());
    }

    // CPU metrics
    try {
        auto cpuStats = MacOsSystemResourcesReader::readCpuStats();
        auto totalStats = cpuStats.getTotal();
        output.totalCPUJiffies = totalStats.user + totalStats.system + totalStats.idle;
        output.cpuCoreNum = MacOsSystemResourcesReader::readCpuStats().size() - 1;

        // TODO: cpu.cfs_period_us
        NES_ERROR("MacOsSystemResourcesReader: File for cpu.cfs_period_us not available");
        output.cpuPeriodUS = 0;

        // TODO: cpu.cfs_quota_us
        NES_ERROR("MacOsSystemResourcesReader: File for cpu.cfs_quota_us not available");
        output.cpuQuotaUS = 0;

    } catch (const log4cxx::helpers::RuntimeException& e) {
        NES_ERROR("MacOsSystemResourcesReader: Error reading static cpu metrics " << e.what());
    }

    return output;
}

CpuMetricsWrapper MacOsSystemResourcesReader::readCpuStats() {
    unsigned int numCpuMetrics = std::thread::hardware_concurrency() + 1;
    auto cpu = std::vector<CpuMetrics>(numCpuMetrics);

    try {
        NES_DEBUG("LinuxSystemResourcesReader: Reading CPU stats for number of CPUs " << numCpuMetrics);

        natural_t cpu_load_count;
        processor_cpu_load_info_t cpu_load_info;
        mach_msg_type_number_t cpu_load_info_count;

        if (KERN_SUCCESS == host_processor_info(mach_host_self(), PROCESSOR_CPU_LOAD_INFO, &cpu_load_count, (processor_info_array_t *)&cpu_load_info, &cpu_load_info_count)) {

            for(natural_t i = 0; i <= cpu_load_count; i++){
                auto cpuStats = CpuMetrics();

                cpuStats.core_num = i+1;
                cpuStats.user = cpu_load_info[i].cpu_ticks[CPU_STATE_USER];
                cpuStats.nice = cpu_load_info[i].cpu_ticks[CPU_STATE_NICE];
                cpuStats.system = cpu_load_info[i].cpu_ticks[CPU_STATE_SYSTEM];
                cpuStats.idle = cpu_load_info[i].cpu_ticks[CPU_STATE_IDLE];
                // TODO: iowait,irq, softirq, steal, guest, guestnice
                cpuStats.iowait = 0;
                cpuStats.irq = 0;
                cpuStats.softirq = 0;
                cpuStats.steal = 0;
                cpuStats.guest = 0;
                cpuStats.guestnice = 0;

                cpu[i+1] = cpuStats;

                cpu[0].user += cpuStats.user;
                cpu[0].nice += cpuStats.nice;
                cpu[0].system += cpuStats.system;
                cpu[0].idle += cpuStats.idle;
                cpu[0].iowait += cpuStats.iowait;
                cpu[0].irq += cpuStats.irq;
                cpu[0].softirq += cpuStats.softirq;
                cpu[0].steal += cpuStats.steal;
                cpu[0].guest += cpuStats.guest;
                cpu[0].guestnice += cpuStats.guestnice;
            }
        }
    } catch (const log4cxx::helpers::RuntimeException& e) {
        NES_ERROR("MacOsSystemResourcesReader: Error calling readCpuStats() " << e.what());
    }

    return CpuMetricsWrapper{std::move(cpu)};
}

NetworkMetricsWrapper MacOsSystemResourcesReader::readNetworkStats() {
    auto output = NetworkMetricsWrapper();

    try {
        NES_DEBUG("MacOsSystemResourcesReader: Reading network stats.");
        int mib[] = {CTL_NET, PF_ROUTE, 0, 0, NET_RT_IFLIST2, 0};

        size_t len;
        if (sysctl(mib, 6, NULL, &len, NULL, 0) < 0) {
            NES_ERROR("MacOsSystemResourcesReader: Reading network stats with sysctl didn't work.");
            return output;
        }
        char* buf = (char*) calloc(len, sizeof(char));
        if (sysctl(mib, 6, buf, &len, NULL, 0) < 0) {
            NES_ERROR("MacOsSystemResourcesReader: Reading network stats with sysctl didn't work.");
            return output;
        }

        char* lim = buf + len;
        char* next = NULL;
        struct if_msghdr* ifm;
        char name[20];

        uint64_t i = 0;
        for (next = buf; next < lim;) {
            ifm = (struct if_msghdr*) next;
            next += ifm->ifm_msglen;
            if (ifm->ifm_type == RTM_IFINFO2) {
                struct if_msghdr2 *if2m = (struct if_msghdr2*) ifm;
                struct sockaddr_dl *sdl = (struct sockaddr_dl*) (if2m + 1);

                strncpy(name, sdl->sdl_data, sdl->sdl_nlen);
                name[sdl->sdl_nlen] = '\0';

                // TODO: FIFO, rFrame, rCompressed, tDrop, tCarrier, tCompressed
                auto outputValue = NetworkMetrics();

                //outputValue.interfaceName = name;// TODO: interfaceName is int (see issue #1725)
                outputValue.interfaceName = i++;

                // the received metric fields
                outputValue.rBytes = if2m->ifm_data.ifi_ibytes;
                outputValue.rPackets = if2m->ifm_data.ifi_ipackets;
                outputValue.rErrs = if2m->ifm_data.ifi_ierrors;
                outputValue.rDrop = if2m->ifm_data.ifi_iqdrops;
                outputValue.rFifo = 0;
                outputValue.rFrame = 0;
                outputValue.rCompressed = 0;
                outputValue.rMulticast = if2m->ifm_data.ifi_imcasts;

                // the transmitted metric fields
                outputValue.tBytes = if2m->ifm_data.ifi_obytes;
                outputValue.tPackets = if2m->ifm_data.ifi_opackets;
                outputValue.tErrs = if2m->ifm_data.ifi_oerrors;
                outputValue.tDrop = 0;
                outputValue.tFifo = 0;
                outputValue.tColls = if2m->ifm_data.ifi_collisions;
                outputValue.tCarrier = 0;
                outputValue.tCompressed = 0;

                // extension of the wrapper class object
                output.addNetworkMetrics(std::move(outputValue));
            }
        }
        free(buf);

    } catch (const log4cxx::helpers::RuntimeException& e) {
        NES_ERROR("MacOsSystemResourcesReader: Error reading network stats " << e.what());
    }
    return output;
}

MemoryMetrics MacOsSystemResourcesReader::readMemoryStats() {
    auto output = MemoryMetrics();

    try {
        NES_DEBUG("MacOsSystemResourcesReader: Reading memory stats.");

        // RAM
        int mib[2] = {CTL_HW, HW_MEMSIZE};
        uint64_t physical_memory;
        size_t length = sizeof(physical_memory);
        if (sysctl(mib, 2, &physical_memory, &length, NULL, 0) < 0){
            NES_ERROR("MacOsSystemResourcesReader: Reading memory with sysctl didn't work");
            output.TOTAL_RAM = 0;
        } else{
            output.TOTAL_RAM = physical_memory;
        }

        vm_size_t page_size;
        vm_statistics_data_t vm_stat;
        mach_msg_type_number_t count = sizeof(vm_stat) / sizeof(natural_t);
        if (KERN_SUCCESS == host_page_size(mach_host_self(), &page_size)
            && KERN_SUCCESS == host_statistics(mach_host_self(), HOST_VM_INFO, (host_info_t)&vm_stat, &count)) {
            output.FREE_RAM = (uint64_t)vm_stat.free_count * (uint64_t)page_size;
        } else {
            NES_ERROR("MacOsSystemResourcesReader: Reading memory with host_statistics didn't work");
            output.FREE_RAM = 0;
        }

        // Virtual Memory
        mib[0] = CTL_VM;
        mib[1] = VM_SWAPUSAGE;
        struct xsw_usage swap;
        length = sizeof(struct xsw_usage);
        if (sysctl(mib, 2, &swap, &length, NULL, 0) < 0) {
            NES_ERROR("MacOsSystemResourcesReader: Reading memory with sysctl didn't work");
            output.TOTAL_SWAP = 0;
            output.FREE_SWAP = 0;
        } else {
            output.TOTAL_SWAP = swap.xsu_total;
            output.FREE_SWAP = swap.xsu_avail;
        }

        // Processes
        int mib_three[3] = {CTL_KERN, KERN_PROC, KERN_PROC_ALL};
        if (sysctl(mib_three, 3, NULL, &length, NULL, 0) < 0) {
            NES_ERROR("MacOsSystemResourcesReader: Reading memory with sysctl didn't work");
            output.PROCS = 0;
        } else {
            output.PROCS = length / sizeof(struct kinfo_proc);
        }

        // TODO: SHARED_RAM, BUFFER_RAM
        output.SHARED_RAM = 0;
        output.BUFFER_RAM = 0;
        output.TOTAL_HIGH = 0;
        output.FREE_HIGH = 0;
        output.MEM_UNIT = 0;

        double loads[3];
        if (getloadavg(loads, 3) < 0) {
            NES_ERROR("MacOsSystemResourcesReader: Reading memory stats with getloadavg didn't work.");
            output.LOADS_1MIN = 0;
            output.LOADS_5MIN = 0;
            output.LOADS_15MIN = 0;
        } else{
            output.LOADS_1MIN = loads[0];
            output.LOADS_5MIN = loads[1];
            output.LOADS_15MIN = loads[2];
        }
    } catch (const log4cxx::helpers::RuntimeException& e) {
        NES_ERROR("MacOsSystemResourcesReader: Error reading memory stats " << e.what());
    }
    return output;
}

DiskMetrics MacOsSystemResourcesReader::readDiskStats() {
    DiskMetrics output{};

    try {
        NES_DEBUG("MacOsSystemResourcesReader: Reading disk stats.");
        auto* svfs = (struct statvfs*) malloc(sizeof(struct statvfs));

        int ret = statvfs("/", svfs);
        if (ret == EFAULT) {
            NES_THROW_RUNTIME_ERROR("MacOsSystemResourcesReader: Error reading disk stats");
        }

        output.fBsize = svfs->f_bsize;
        output.fFrsize = svfs->f_frsize;
        output.fBlocks = svfs->f_blocks;
        output.fBfree = svfs->f_bfree;
        output.fBavail = svfs->f_bavail;
    } catch (const log4cxx::helpers::RuntimeException& e) {
        NES_ERROR("MacOsSystemResourcesReader: Error reading disk stats " << e.what());
    }
    return output;
}

uint64_t MacOsSystemResourcesReader::getWallTimeInNs() {
    auto now = std::chrono::system_clock::now();
    auto now_s = std::chrono::time_point_cast<std::chrono::nanoseconds>(now);
    auto epoch = now_s.time_since_epoch();
    auto value = std::chrono::duration_cast<std::chrono::nanoseconds>(epoch);
    uint64_t duration = value.count();
    return duration;
}
}// namespace NES