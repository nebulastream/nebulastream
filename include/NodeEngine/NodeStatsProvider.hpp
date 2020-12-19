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

#ifndef _NODEENGINE_NODESTATSPROVIDER_HPP
#define _NODEENGINE_NODESTATSPROVIDER_HPP

#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <NodeStats.pb.h>
#include <memory>

namespace NES::NodeEngine {

/**
 * @brief: This class gathers the properties of a node.
 */
class NodeStatsProvider {
  public:
    /**
    * @brief create a new NodeProperties object
    * @param default 0 cpus
    */
    NodeStatsProvider();

    /**
     * @brief Factory to create a node property pointer
     * @return
     */
    static NodeStatsProviderPtr create();

    ~NodeStatsProvider() = default;

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

typedef std::shared_ptr<NodeStatsProvider> NodeStatsProviderPtr;

}// namespace NES::NodeEngine
#endif
