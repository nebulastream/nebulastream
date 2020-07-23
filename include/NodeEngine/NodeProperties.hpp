#ifndef _METRICS_H
#define _METRICS_H


#include <NodeStats.pb.h>
#include <memory>

namespace NES {

class NodeProperties;
typedef std::shared_ptr<NodeProperties> NodePropertiesPtr;

/**
 * @brief: This class gathers the properties of a node.
 */
class NodeProperties {
  public:
    /**
    * @brief create a new NodeProperties object
    * @param default 0 cpus
    */
    NodeProperties();

    /**
     * @brief Factory to create a node property pointer
     * @return
     */
    static NodePropertiesPtr create();


    ~NodeProperties() = default;


    /**
    * @brief gets the node stats.
    */
    NodeStats getNodeStats();

    /**
     * @brief gathers CPU, MEM, Disk, and Network state.
     */
    void update();

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
    * @brief set client name manually (for testing locally)
    * @param client name as string
    */
    void setClientName(std::string clientName);

    /**
    * @brief set client port manually (for testing locally)
    * @return client port as string
    */
    void setClientPort(std::string clientPort);

    std::string getClientName();
    std::string getClientPort();
  private:
    long nbrProcessors;
    NodeStats nodeStats;
    std::string clientName;
    std::string clientPort;
};

typedef std::shared_ptr<NodeProperties> NodePropertiesPtr;

}// namespace NES
#endif
