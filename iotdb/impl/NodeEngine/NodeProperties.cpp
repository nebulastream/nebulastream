#include <NodeEngine/NodeProperties.hpp>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
namespace NES {

void NodeProperties::readCpuStats() {

    this->_cpus.clear();

    std::ifstream fileStat("/proc/stat");
    std::string line;
    std::string token;

    while (std::getline(fileStat, line)) {
        // line starts with "cpu"
        if (!line.compare(0, 3, "cpu")) {
            std::istringstream ss(line);
            std::vector<std::string> tokens{
                std::istream_iterator<std::string>{ss},
                std::istream_iterator<std::string>{}};

            // check columns
            if (tokens.size() != 11) {
                std::cerr << "ERROR: /proc/stat incorrect" << std::endl;
            }
            JSON cpu;

            char name[8];
            int len = tokens[0].copy(name, tokens[0].size());
            name[len] = '\0';

            cpu["name"] = name;
            cpu["user"] = std::stoul(tokens[1]);
            cpu["nice"] = std::stoul(tokens[2]);
            cpu["system"] = std::stoul(tokens[3]);
            cpu["idle"] = std::stoul(tokens[4]);
            cpu["iowait"] = std::stoul(tokens[5]);
            cpu["irq"] = std::stoul(tokens[6]);
            cpu["softirq"] = std::stoul(tokens[7]);
            cpu["steal"] = std::stoul(tokens[8]);
            cpu["guest"] = std::stoul(tokens[9]);
            cpu["guest_nice"] = std::stoul(tokens[10]);

            this->_cpus.push_back(cpu);
        }
    }
    this->nbrProcessors = this->_cpus.size() - 1;
    this->_metrics["cpus"] = this->_cpus;
}

void NodeProperties::loadExistingProperties(std::string props) {
    set(props.c_str());
}

std::string NodeProperties::getCpuStats() {
    return _cpus.dump();
}
std::string NodeProperties::getNetworkStats() {
    return _nets.dump();
}
std::string NodeProperties::getMemStats() {
    return _mem.dump();
}

std::string NodeProperties::getDiskStats() {
    return _disk.dump();
}

std::string NodeProperties::getMetric() {
    return _metrics.dump();
}

void NodeProperties::setClientName(std::string pClientName) {
    clientName = pClientName;
};

void NodeProperties::setClientPort(std::string pClientPort) {
    clientPort = pClientPort;
};

std::string NodeProperties::getClientName() {
    std::string host = _nets[0]["hostname"].dump();
    host.erase(0, 1);
    host.erase(host.length() - 1, 1);
    return host;
}

std::string NodeProperties::getClientPort() {
    std::string port = _nets[0]["port"].dump();
    port.erase(0, 1);
    port.erase(port.length() - 1, 1);
    return port;
}

#if defined(__linux__)
void NodeProperties::readNetworkStats() {
    this->_nets.clear();
    char hostnameChar[1024];
    gethostname(hostnameChar, 1024);
    std::string s1 = hostnameChar;
    JSON hname;

    hname["hostname"] = clientName;
    hname["port"] = clientPort;
    this->_nets.push_back(hname);

    struct ifaddrs* ifa;

    int family, s;
    char host[NI_MAXHOST];
    struct ifaddrs* ifaddr;
    if (getifaddrs(&ifaddr) == -1) {
        std::cerr << "ERROR: getifaddrs failed" << std::endl;
        return;
    }

    struct ifreq ifr;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
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

        JSON net;
        auto found = keeper.find(ifa->ifa_name);
        if (found != keeper.cend()) {
            net = this->_nets.at(found->second);
        } else {
            keeper[ifa->ifa_name] = n;
            n++;
        }

        if (family == AF_INET) {
            s = getnameinfo(ifa->ifa_addr,
                            sizeof(struct sockaddr_in),
                            host, NI_MAXHOST,
                            NULL, 0, NI_NUMERICHOST);
            if (s != 0) {
                std::cerr << "ERROR: getnameinfo failed" << std::endl;
                continue;
            }
            net[ifa->ifa_name]["host"] = host;
        } else if (family == AF_INET6) {
            s = getnameinfo(ifa->ifa_addr,
                            sizeof(struct sockaddr_in6),
                            host, NI_MAXHOST,
                            NULL, 0, NI_NUMERICHOST);
            if (s != 0) {
                std::cerr << "ERROR: getnameinfo failed" << std::endl;
                continue;
            }
            net[ifa->ifa_name]["host6"] = host;

        } else if (family == AF_PACKET && ifa->ifa_data != NULL) {
            struct rtnl_link_stats* stats = (struct rtnl_link_stats*) (ifa->ifa_data);
            net[ifa->ifa_name]["tx_packets"] = stats->tx_packets;
            net[ifa->ifa_name]["tx_bytes"] = stats->tx_bytes;
            net[ifa->ifa_name]["tx_dropped"] = stats->tx_dropped;
            net[ifa->ifa_name]["rx_packets"] = stats->rx_packets;
            net[ifa->ifa_name]["rx_bytes"] = stats->rx_bytes;
            net[ifa->ifa_name]["rx_dropped"] = stats->rx_dropped;
        }
        if (found != keeper.cend()) {
            this->_nets[found->second] = net;
        } else {
            this->_nets.push_back(net);
        }
    }

    this->_metrics["nets"] = this->_nets;
}
#elif defined(__APPLE__) || defined(__MACH__)
void NodeProperties::readNetworkStats() {
}
#else
#error "Unsupported platform"
#endif

void NodeProperties::print() {
    std::cout << "cpu stats=" << std::endl;
    std::cout << getCpuStats() << std::endl;

    std::cout << "network stats=" << std::endl;
    std::cout << getNetworkStats() << std::endl;

    std::cout << "memory stats=" << std::endl;
    std::cout << getMemStats() << std::endl;

    std::cout << "filesystem stats=" << std::endl;
    std::cout << getDiskStats() << std::endl;
}
#if defined(__linux__)
void NodeProperties::readMemStats() {
    this->_mem.clear();

    struct sysinfo* sinfo = (struct sysinfo*) malloc(sizeof(struct sysinfo));

    int ret = sysinfo(sinfo);
    if (ret == EFAULT)
        perror("ERROR: read filesystem ");

    this->_mem["totalram"] = sinfo->totalram;
    this->_mem["totalswap"] = sinfo->totalswap;
    this->_mem["freeram"] = sinfo->freeram;
    this->_mem["sharedram"] = sinfo->sharedram;
    this->_mem["bufferram"] = sinfo->bufferram;
    this->_mem["freeswap"] = sinfo->freeswap;
    this->_mem["totalhigh"] = sinfo->totalhigh;
    this->_mem["freehigh"] = sinfo->freehigh;
    this->_mem["procs"] = sinfo->procs;
    this->_mem["mem_unit"] = sinfo->mem_unit;
    this->_mem["loads_1min"] = sinfo->loads[0];
    this->_mem["loads_5min"] = sinfo->loads[1];
    this->_mem["loads_15min"] = sinfo->loads[2];

    this->_metrics["mem"] = this->_mem;
}
#else
void NodeProperties::readMemStats() {
}
#endif

#if defined(__linux__)
void NodeProperties::readDiskStats() {
    // this->_cpus = JSON({});
    this->_disk.clear();
    struct statvfs* svfs = (struct statvfs*) malloc(sizeof(struct statvfs));

    int ret = statvfs("/", svfs);
    if (ret == EFAULT)
        perror("ERROR: read filesystem ");

    this->_disk["f_bsize"] = svfs->f_bsize;
    this->_disk["f_frsize"] = svfs->f_frsize;
    this->_disk["f_blocks"] = svfs->f_blocks;
    this->_disk["f_bfree"] = svfs->f_bfree;
    this->_disk["f_bavail"] = svfs->f_bavail;

    this->_metrics["fs"] = this->_disk;
}
#else
void NodeProperties::readDiskStats() {
}
#endif
std::string NodeProperties::dump(int setw) {
    return _metrics.dump(setw);
}

void NodeProperties::set(const char* metricsBuffer) {
    this->_metrics = JSON::parse(metricsBuffer);

    _mem = this->_metrics["mem"];
    _disk = this->_metrics["fs"];
    _cpus = this->_metrics["cpus"];
    _nets = this->_metrics["nets"];
}

JSON NodeProperties::getExistingMetrics() {
    return this->_metrics;
}
}// namespace NES
