#include "../include/NodeEngine/NodeProperties.hpp"

void NodeProperties::readCpuStats()
{

    this->_cpus.clear();

    std::ifstream fileStat("/proc/stat");
    std::string line;
    std::string token;

    while (std::getline(fileStat, line)) {
        // line starts with "cpu"
        if (!line.compare(0, 3, "cpu")) {
            std::istringstream ss(line);
            std::vector<std::string> tokens{std::istream_iterator<std::string>{ss},
                                            std::istream_iterator<std::string>{}};

            // check columns
            if (tokens.size() != PROC_STAT_CPU_COLUMNS) {
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

void NodeProperties::readNetworkStats()
{
    this->_nets.clear();

    struct ifaddrs* ifa;

    int family, s;
    char host[NI_MAXHOST];
    if (getifaddrs(&this->ifaddr) == -1) {
        std::cerr << "ERROR: getifaddrs failed" << std::endl;
        return;
    }

    struct ifreq ifr;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    std::map<std::string, int> keeper;

    int n = 0;
    for (ifa = this->ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
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
        }
        else {
            keeper[ifa->ifa_name] = n;
            n++;
        }

        if (family == AF_INET) {
            s = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in), host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
            if (s != 0) {
                std::cerr << "ERROR: getnameinfo failed" << std::endl;
                continue;
            }
            net[ifa->ifa_name]["host"] = host;
        }
        else if (family == AF_INET6) {
            s = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in6), host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
            if (s != 0) {
                std::cerr << "ERROR: getnameinfo failed" << std::endl;
                continue;
            }
            net[ifa->ifa_name]["host6"] = host;
        }
        else if (family == AF_PACKET && ifa->ifa_data != NULL) {
            struct rtnl_link_stats* stats = (struct rtnl_link_stats*)(ifa->ifa_data);
            net[ifa->ifa_name]["tx_packets"] = stats->tx_packets;
            net[ifa->ifa_name]["tx_bytes"] = stats->tx_bytes;
            net[ifa->ifa_name]["tx_dropped"] = stats->tx_dropped;
            net[ifa->ifa_name]["rx_packets"] = stats->rx_packets;
            net[ifa->ifa_name]["rx_bytes"] = stats->rx_bytes;
            net[ifa->ifa_name]["rx_dropped"] = stats->rx_dropped;
        }
        if (found != keeper.cend()) {
            this->_nets[found->second] = net;
        }
        else {
            this->_nets.push_back(net);
        }
    }

    this->_metrics["nets"] = this->_nets;
}

void NodeProperties::readMemStats()
{
    this->_mem.clear();

    if (this->sinfo == NULL)
        this->sinfo = (struct sysinfo*)malloc(sizeof(struct sysinfo));
    int ret = sysinfo(this->sinfo);
    if (ret == EFAULT)
        perror("ERROR: read filesystem ");

    this->_mem["totalram"] = this->sinfo->totalram;
    this->_mem["totalswap"] = this->sinfo->totalswap;
    this->_mem["freeram"] = this->sinfo->freeram;
    this->_mem["sharedram"] = this->sinfo->sharedram;
    this->_mem["bufferram"] = this->sinfo->bufferram;
    this->_mem["freeswap"] = this->sinfo->freeswap;
    this->_mem["totalhigh"] = this->sinfo->totalhigh;
    this->_mem["freehigh"] = this->sinfo->freehigh;
    this->_mem["procs"] = this->sinfo->procs;
    this->_mem["mem_unit"] = this->sinfo->mem_unit;
    this->_mem["loads_1min"] = this->sinfo->loads[0];
    this->_mem["loads_5min"] = this->sinfo->loads[1];
    this->_mem["loads_15min"] = this->sinfo->loads[2];

    this->_metrics["mem"] = this->_mem;
}

void NodeProperties::readFsStats()
{
    // this->_cpus = JSON({});
    this->_fs.clear();

    if (this->svfs == NULL)
        this->svfs = (struct statvfs*)malloc(sizeof(struct statvfs));

    int ret = statvfs("/", this->svfs);
    if (ret == EFAULT)
        perror("ERROR: read filesystem ");

    this->_fs["f_bsize"] = this->svfs->f_bsize;
    this->_fs["f_frsize"] = this->svfs->f_frsize;
    this->_fs["f_blocks"] = this->svfs->f_blocks;
    this->_fs["f_bfree"] = this->svfs->f_bfree;
    this->_fs["f_bavail"] = this->svfs->f_bavail;

    this->_metrics["fs"] = this->_fs;
}

std::string NodeProperties::dump(int setw) { return _metrics.dump(setw); }

JSON NodeProperties::load(const char* metricsBuffer)
{
    this->_metrics = JSON::parse(metricsBuffer);
    return this->_metrics;
}

JSON NodeProperties::load() { return this->_metrics; }
