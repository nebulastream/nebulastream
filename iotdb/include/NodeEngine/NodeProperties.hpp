#ifndef _METRICS_H
#define _METRICS_H

#include <sys/sysinfo.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <ifaddrs.h>
#include <linux/if_link.h>
#include <netdb.h>
#include <net/if.h>

#include <fstream>
#include "json.hpp"

namespace iotdb{
using JSON = nlohmann::json;
typedef unsigned long uint64_t;

#define PROC_STAT_CPU_COLUMNS 11

class NodeProperties {
public:
  NodeProperties() {}

  ~NodeProperties() {
    if (this->sinfo)
      free(this->sinfo);
    if (this->svfs)
      free(this->svfs);
    if (this->ifaddr)
      freeifaddrs(this->ifaddr);
  }

  void print();
  std::string dump(int setw=-1);
  JSON load(const char *metricsBuffer);
  JSON load();
public:
  void readCpuStats();           // read cpu inforamtion
  void readMemStats();           // read memory information
  void readFsStats();            // read file system information
  void readNetworkStats();       // read network information

  std::string getCpuStats();
  std::string getNetworkStats();
  std::string getMemStats();
  std::string getFsStats();

private:
  /*
   * memory information
   * /proc/meminfo
   * /proc/loadavg
   */
  struct sysinfo *sinfo;
  /*
   * file system information
   * /proc/diskstats
   */
  struct statvfs *svfs;
  /*
   * network information
   * /proc/net/dev
   *
   */
  struct ifaddrs *ifaddr;

  long nbrProcessors;

  JSON _metrics;
  JSON _mem;
  JSON _fs;
  JSON _cpus;
  JSON _nets;
};
}
#endif
