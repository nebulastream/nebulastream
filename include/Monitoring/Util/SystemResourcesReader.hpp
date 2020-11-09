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

#ifndef NES_INCLUDE_RUNTIME_UTIL_SYSTEMRESOURCESCOUNTER_HPP_
#define NES_INCLUDE_RUNTIME_UTIL_SYSTEMRESOURCESCOUNTER_HPP_

#include <memory>
#include <string>
#include <unordered_map>

namespace NES {

class CpuMetrics;
class MemoryMetrics;
class NetworkMetrics;
class DiskMetrics;

/**
 * @brief This is a static utility class to collect basic system information
 * Warning: Only Linux distributions are currently supported
 */
class SystemResourcesReader {
  public:
    /**
     * @brief This method reads CPU information from /proc/stat.
     * @return A map where for each CPU the according /proc/stat information are returned in the form
     * e.g., output["user1"] = 1234, where user is the metric and 1 the cpu core
     */
    static CpuMetrics ReadCPUStats();

    /**
     * @brief This method reads memory information from sysinfo
     * @return A map with the memory information
     */
    static MemoryMetrics ReadMemoryStats();

    /**
     * @brief This method reads disk stats from statvfs
     * @return A map with the disk stats
     */
    static DiskMetrics ReadDiskStats();

    /**
     * @brief This methods reads network statistics from /proc/net/dev and returns them for each interface in a
     * separate map
     * @return a map where each interface is mapping the according network statistics map.
     */
    static NetworkMetrics ReadNetworkStats();

  private:
    SystemResourcesReader() = default;
};

}// namespace NES

#endif//NES_INCLUDE_RUNTIME_UTIL_SYSTEMRESOURCESCOUNTER_HPP_
