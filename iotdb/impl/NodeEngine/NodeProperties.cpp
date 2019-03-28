#include <NodeEngine/NodeProperties.hpp>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
namespace iotdb{
void NodeProperties::readCpuStats() {

  this->_cpus.clear();

  std::ifstream fileStat("/proc/stat");
  std::string line;
  std::string token;

  while (std::getline(fileStat, line)) {
    // line starts with "cpu"
    if (! line.compare(0, 3, "cpu")) {
      std::istringstream ss(line);
      std::vector<std::string> tokens{
        std::istream_iterator<std::string>{ss},
          std::istream_iterator<std::string>{}
      };

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

std::string NodeProperties::getCpuStats()
{
    return _cpus.dump();
}
std::string NodeProperties::getNetworkStats()
{
    return _nets.dump();
}
std::string NodeProperties::getMemStats()
{
    return _mem.dump();
}

std::string NodeProperties::getFsStats()
{
    return _fs.dump();
}

std::string NodeProperties::getMetric()
{
    return _metrics.dump();
}

std::string NodeProperties::getHostname()
{
    std::string host = _nets[0]["hostname"].dump();
    host.erase(0,1);               //           ^
    host.erase(host.length()-1,1);               //           ^
    return host;

}


void NodeProperties::readNetworkStats() {
  this->_nets.clear();
  char hostname[1024];
  gethostname(hostname, 1024);
  std::string s1 = hostname;
  JSON hname;
  hname["hostname"] = s1;
  this->_nets.push_back(hname);

  struct ifaddrs* ifa;

  int family, s;
  char host[NI_MAXHOST];
  struct ifaddrs *ifaddr;
  if (getifaddrs(&ifaddr) == -1) {
    std::cerr << "ERROR: getifaddrs failed" << std::endl;
    return ;
  }

  struct ifreq ifr;
  int fd = socket(AF_INET, SOCK_DGRAM, 0);
  std::map<std::string, int> keeper;

  int n = 0;
  for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == NULL)
      continue;

    strncpy(ifr.ifr_name , ifa->ifa_name , IFNAMSIZ-1);
    ioctl(fd, SIOCGIFFLAGS, &ifr);

    if ( ! (ifr.ifr_flags & IFF_UP) ||
        (ifr.ifr_flags & IFF_LOOPBACK))
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
      struct rtnl_link_stats *stats = (struct rtnl_link_stats *)(ifa->ifa_data);
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

void NodeProperties::print()
{
    std::cout << "cpu stats=" << std::endl;
    std::cout << getCpuStats() << std::endl;

    std::cout << "network stats=" << std::endl;
    std::cout << getNetworkStats() << std::endl;

    std::cout << "mbemory stats=" << std::endl;
    std::cout << getMemStats() << std::endl;

    std::cout << "filesystem stats=" << std::endl;
    std::cout << getFsStats() << std::endl;
}
void NodeProperties::readMemStats() {
  this->_mem.clear();

  struct sysinfo* sinfo = (struct sysinfo *)malloc(sizeof(struct sysinfo));

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

void NodeProperties::readFsStats() {
  // this->_cpus = JSON({});
  this->_fs.clear();
  struct statvfs *svfs = (struct statvfs *)malloc(sizeof(struct statvfs));

  int ret = statvfs("/", svfs);
  if (ret == EFAULT)
    perror("ERROR: read filesystem ");

  this->_fs["f_bsize"] = svfs->f_bsize;
  this->_fs["f_frsize"] = svfs->f_frsize;
  this->_fs["f_blocks"] = svfs->f_blocks;
  this->_fs["f_bfree"] = svfs->f_bfree;
  this->_fs["f_bavail"] = svfs->f_bavail;

  this->_metrics["fs"] = this->_fs;
}

std::string NodeProperties::dump(int setw) {
  return _metrics.dump(setw);
}

void NodeProperties::load(const char* metricsBuffer) {
  this->_metrics = JSON::parse(metricsBuffer);

  _mem = this->_metrics["mem"];
  _fs = this->_metrics["fs"];
  _cpus = this->_metrics["cpus"];
  _nets = this->_metrics["nets"];
}

JSON NodeProperties::load() {
  return this->_metrics;
}
}
