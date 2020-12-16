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

#if defined(__linux__)
#include <ifaddrs.h>
#include <linux/if_link.h>
#include <net/if.h>
#include <netdb.h>
#include <string>
#include <sys/ioctl.h>
#include <sys/statvfs.h>
#include <sys/sysinfo.h>
#elif defined(__APPLE__) || defined(__MACH__)
#else
#error "Unsupported platform"
#endif
#include <NodeEngine/NodeStatsProvider.hpp>
#include <Util/Logger.hpp>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <utility>

namespace NES {

NodeStatsProvider::NodeStatsProvider() : nbrProcessors(0), nodeStats(NodeStats()) {}

NodeStatsProviderPtr NodeStatsProvider::create() { return std::make_shared<NodeStatsProvider>(); }

void NodeStatsProvider::update() {
    nodeStats.Clear();
    readCpuStats();
    readMemStats();
    readDiskStats();
    readNetworkStats();
}

void NodeStatsProvider::readCpuStats() {

    auto cpuStats = nodeStats.mutable_cpustats();
    cpuStats->Clear();

    std::ifstream fileStat("/proc/stat");
    std::string line;
    auto numberOfPreccessors = 0;
    while (std::getline(fileStat, line)) {
        // line starts with "cpu"
        if (!line.compare(0, 3, "cpu")) {
            std::istringstream ss(line);
            std::vector<std::string> tokens{std::istream_iterator<std::string>{ss}, std::istream_iterator<std::string>{}};

            // check columns
            if (tokens.size() != 11) {
                NES_ERROR("NodeProperties: could not read CPU statistics: /proc/stat incorrect");
            }
            auto cpuStat = cpuStats->Add();

            char name[8];
            int len = tokens[0].copy(name, tokens[0].size());
            name[len] = '\0';

            cpuStat->set_name(name);
            cpuStat->set_user(std::stoul(tokens[1]));
            cpuStat->set_system(std::stoul(tokens[2]));
            cpuStat->set_iowait(std::stoul(tokens[3]));
            cpuStat->set_idle(std::stoul(tokens[4]));
            cpuStat->set_iowait(std::stoul(tokens[5]));
            cpuStat->set_irq(std::stoul(tokens[6]));
            cpuStat->set_steal(std::stoul(tokens[7]));
            cpuStat->set_gust(std::stoul(tokens[8]));
            cpuStat->set_gust_nice(std::stoul(tokens[9]));
            numberOfPreccessors++;
        }
    }
    this->nbrProcessors = numberOfPreccessors;
}

void NodeStatsProvider::setClientName(std::string clientName) { this->clientName = std::move(clientName); };

void NodeStatsProvider::setClientPort(std::string clientPort) { this->clientPort = std::move(clientPort); };

std::string NodeStatsProvider::getClientName() {
    std::string host = clientName;
    host.erase(0, 1);
    host.erase(host.length() - 1, 1);
    return host;
}

std::string NodeStatsProvider::getClientPort() {
    std::string port = nodeStats.networkstats().port();
    port.erase(0, 1);
    port.erase(port.length() - 1, 1);
    return port;
}

#if defined(__linux__)
void NodeStatsProvider::readNetworkStats() {
    auto networkStats = nodeStats.mutable_networkstats();
    networkStats->Clear();

    char hostnameChar[1024];
    gethostname(hostnameChar, 1024);
    std::string s1 = hostnameChar;
    networkStats->set_hostname(hostnameChar);
    networkStats->set_port(clientPort);
    struct ifaddrs* ifa;

    int family, s;
    char host[NI_MAXHOST];
    struct ifaddrs* ifaddr;
    if (getifaddrs(&ifaddr) == -1) {
        NES_ERROR("NodeProperties: could not read Network statistics: getifaddrs failed");
        return;
    }

    struct ifreq ifr;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);

    // map to keep track of interface names, to assign all properties to the correct interface.
    std::map<std::string, int> keeper;

    int n = 0;
    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL)
            continue;

        strncpy(ifr.ifr_name, ifa->ifa_name, IFNAMSIZ - 1);
        ioctl(fd, SIOCGIFFLAGS, &ifr);

        if (!(ifr.ifr_flags & IFF_UP) || (ifr.ifr_flags & IFF_LOOPBACK))
            continue;

        family = ifa->ifa_addr->sa_family;

        NodeStats_NetworkStats_Interface* interface;
        // check if the interface was already found before
        auto found = keeper.find(ifa->ifa_name);
        if (found != keeper.cend()) {
            interface = networkStats->mutable_interfaces(found->second);
        } else {
            keeper[ifa->ifa_name] = n;
            n++;
            interface = networkStats->mutable_interfaces()->Add();
        }

        // set the interface name
        interface->set_name(ifa->ifa_name);

        if (family == AF_INET) {
            s = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in), host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
            if (s != 0) {
                NES_ERROR("NodeProperties: could not read Network statistics: getnameinfo failed");
                continue;
            }
            interface->set_host(host);
        } else if (family == AF_INET6) {
            s = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in6), host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
            if (s != 0) {
                NES_ERROR("NodeProperties: could not read Network statistics: getnameinfo failed");
                continue;
            }
            interface->set_host6(host);
        } else if (family == AF_PACKET && ifa->ifa_data != NULL) {
            struct rtnl_link_stats* stats = (struct rtnl_link_stats*) (ifa->ifa_data);
            interface->set_tx_packets(stats->tx_packets);
            interface->set_tx_bytes(stats->tx_bytes);
            interface->set_tx_dropped(stats->tx_packets);
            interface->set_tx_packets(stats->tx_packets);
            interface->set_rx_bytes(stats->rx_bytes);
            interface->set_rx_dropped(stats->rx_dropped);
        }
    }
}
#elif defined(__APPLE__) || defined(__MACH__)
void NodeStatsProvider::readNetworkStats() {}
#else
#error "Unsupported platform"
#endif

#if defined(__linux__)
void NodeStatsProvider::readMemStats() {
    auto memoryStats = nodeStats.mutable_memorystats();
    memoryStats->Clear();

    auto* sinfo = (struct sysinfo*) malloc(sizeof(struct sysinfo));

    auto result = sysinfo(sinfo);
    if (result == EFAULT) {
        NES_ERROR("NodeProperties: could not read Disk statistics");
    } else {
        memoryStats->set_totalram(sinfo->totalram);
        memoryStats->set_totalswap(sinfo->totalswap);
        memoryStats->set_freeram(sinfo->freeram);
        memoryStats->set_sharedram(sinfo->sharedram);
        memoryStats->set_bufferram(sinfo->bufferram);
        memoryStats->set_freeswap(sinfo->freeswap);
        memoryStats->set_totalhigh(sinfo->totalhigh);
        memoryStats->set_freehigh(sinfo->freehigh);
        memoryStats->set_procs(sinfo->procs);
        memoryStats->set_mem_unit(sinfo->mem_unit);
        memoryStats->set_loads_1min(sinfo->loads[0]);
        memoryStats->set_loads_5min(sinfo->loads[1]);
        memoryStats->set_loads_15min(sinfo->loads[2]);
    }
    delete[] sinfo;
}
#else
void NodeStatsProvider::readMemStats() {}
#endif

#if defined(__linux__)
void NodeStatsProvider::readDiskStats() {
    auto diskStates = nodeStats.mutable_diskstats();
    diskStates->Clear();

    struct statvfs* svfs = (struct statvfs*) malloc(sizeof(struct statvfs));

    int ret = statvfs("/", svfs);
    if (ret == EFAULT) {
        NES_ERROR("NodeProperties: could not read Disk statistics");
    } else {
        diskStates->set_f_bsize(svfs->f_bsize);
        diskStates->set_f_frsize(svfs->f_frsize);
        diskStates->set_f_blocks(svfs->f_blocks);
        diskStates->set_f_bfree(svfs->f_bfree);
        diskStates->set_f_bavail(svfs->f_bavail);
    }
    delete[] svfs;
}
#else
void NodeStatsProvider::readDiskStats() {}
#endif

NodeStats NodeStatsProvider::getNodeStats() { return nodeStats; }

}// namespace NES
