#ifndef _METRICS_H
#define _METRICS_H

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
#include "../Util/json.hpp"
#include <fstream>

namespace NES {
using JSON = nlohmann::json;
//typedef unsigned long uint64_t;

/**
 * \brief: This class captures the properties of a node which are send to and updated continuously.
 */
class NodeProperties {
  public:
    /**
   * @brief create a new NodeProperties object
   * @param default 0 cpus
   */
    NodeProperties()
        : nbrProcessors(0){};

    NodeProperties(std::string props) {
        loadExistingProperties(props);
    }

    ~NodeProperties() {
    }

    /**
   * @brief print cpu,network,memory, and filesstem stats on cout
   */
    void print();

    /**
   * @brief dumbs all metrics into a string
   * @param indent not set
   * @return serialized json object as string
   */
    std::string dump(int setw = -1);

    /**
   * @brief set existing serialized node property into nodeProperties.
   * Load entire _metrics and all sub jsons
   * @param char pointer to serialized JSON
   */
    void set(const char* metricsBuffer);

    /**
   * @brief create the properties and return them
   * @return char pointer to serialized JSON
   */
    JSON getExistingMetrics();

    /**
   * @brief load existing properties
   * @param properteis
   */
    void loadExistingProperties(std::string props);

    /**
   * @brief gather cpu information from /proc/stat
   * Gathered stats for each cpu:
   * name, user, nice, system, idle, iowait, irq,
   * softirq, steal, guest, guest_nice
   */
    void readCpuStats();

    /**
   * @brief gather memory information from sysinfo
   * Gathered stats:
   * totalram, totalswap, freeram, sharedram, bufferram,
   * freeswap, totalhigh, freehigh, procs, mem_unit,
   * loads_1min, loads_5min, loads_15min
   */
    void readMemStats();

    /**
   * @brief gather disk information from statvfs
   * Gathered stats:
   * f_bsize, f_frsize, f_blocks, f_bfree, f_bavail
   */
    void readDiskStats();

    /**
   * @brief gather network information from hostname and ifaddrs
   * Gathered stats for each device:
   * hostV4, hostV6, tx_packets, tx_bytes, tx_dropped,
   * rx_packets, rx_bytes, rx_dropped
   * todo: add bandwidth measurements here
   */
    void readNetworkStats();

    /**
   * @brief get CPU stats
   * @return cpu stats as string
   */
    std::string getCpuStats();

    /**
   * @brief get memory stats
   * @return memory stats as string
   */
    std::string getMemStats();

    /**
   * @brief get disk stats
   * @return disk stats as string
   */
    std::string getDiskStats();

    /**
   * @brief get network stats
   * @return network stats as string
   */
    std::string getNetworkStats();

    /**
   * @brief metric summary
   * @return all stats in one JSON file
   */
    std::string getMetric();

    /**
   * @brief get CPU stats
   * @return cpu stats as string
   */
    std::string getClientName();

    /**
   * @brief get CPU stats
   * @return cpu stats as string
   */
    std::string getClientPort();

    /**
   * @brief set client name manually (for testing locally)
   * @param client name as string
   */
    void setClientName(std::string clientname);

    /**
   * @brief set client port manually (for testing locally)
   * @return client port as string
   */
    void setClientPort(std::string clientPort);

  private:
    long nbrProcessors;
    JSON _metrics;
    JSON _mem;
    JSON _disk;
    JSON _cpus;
    JSON _nets;
    std::string clientName;
    std::string clientPort;
};

typedef std::shared_ptr<NodeProperties> NodePropertiesPtr;

}// namespace NES
#endif
