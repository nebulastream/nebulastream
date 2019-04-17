#ifndef _METRICS_H
#define _METRICS_H

#include <ifaddrs.h>
#include <linux/if_link.h>
#include <net/if.h>
#include <netdb.h>
#include <sys/ioctl.h>
#include <sys/statvfs.h>
#include <sys/sysinfo.h>
#include <sys/types.h>

#include "json.hpp"
#include <fstream>

namespace iotdb {
using JSON = nlohmann::json;
typedef unsigned long uint64_t;

#define PROC_STAT_CPU_COLUMNS 11

class NodeProperties {
public:
  NodeProperties():nbrProcessors(0) {};

    ~NodeProperties() {}

  void print();
  std::string dump(int setw=-1);
  void load(const char *metricsBuffer);
  JSON load();

    void readCpuStats();     // read cpu inforamtion
    void readMemStats();     // read memory information
    void readFsStats();      // read file system information
    void readNetworkStats(); // read network information

  std::string getCpuStats();
  std::string getNetworkStats();
  std::string getMemStats();
  std::string getFsStats();
  std::string getMetric();
  std::string getClientName();
  std::string getClientPort();
  void setClientName(std::string clientname);
  void setClientPort(std::string clientPort);


private:
    long nbrProcessors;
    JSON _metrics;
    JSON _mem;
    JSON _fs;
    JSON _cpus;
    JSON _nets;
    std::string clientName;
    std::string clientPort;
};

typedef std::shared_ptr<NodeProperties> NodePropertiesPtr;

}
#endif
