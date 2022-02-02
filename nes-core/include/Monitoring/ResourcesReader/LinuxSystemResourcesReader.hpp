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

#ifndef NES_INCLUDE_MONITORING_UTIL_SYSTEM_RESOURCES_READER_HPP_
#define NES_INCLUDE_MONITORING_UTIL_SYSTEM_RESOURCES_READER_HPP_

#include <Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp>

namespace NES {
class CpuMetrics;
class MemoryMetrics;
class NetworkMetrics;
class DiskMetrics;
class RuntimeNesMetrics;
class StaticNesMetrics;

/**
* @brief This is a static utility class to collect basic system information on a Linux operating System
* Warning: Only Linux distributions are currently supported
*/
class LinuxSystemResourcesReader : public AbstractSystemResourcesReader {
  public:
    LinuxSystemResourcesReader();

    /**
    * @brief This methods reads runtime system metrics that are used within NES (e.g., memory usage, cpu load).
    * @return A RuntimeNesMetrics object containing the metrics.
    */
    RuntimeNesMetrics readRuntimeNesMetrics() override;

    /**
    * @brief This methods reads static system metrics that are used within NES (e.g., totalMemoryBytes, core num.).
    * @return A StaticNesMetrics object containing the metrics.
    */
    StaticNesMetrics readStaticNesMetrics() override;

    /**
    * @brief This method reads CPU information from /proc/stat.
    * Warning: Does not return correct values in containerized environments.
    * @return A map where for each CPU the according /proc/stat information are returned in the form
    * e.g., output["user1"] = 1234, where user is the metric and 1 the cpu core
    */
    CpuMetrics readCpuStats() override;

    /**
    * @brief This method reads memory information from sysinfo
    * Warning: Does not return correct values in containerized environments.
    * @return A map with the memory information
    */
    MemoryMetrics readMemoryStats() override;

    /**
    * @brief This method reads disk stats from statvfs
    * Warning: Does not return correct values in containerized environments.
    * @return A map with the disk stats
    */
    DiskMetrics readDiskStats() override;

    /**
    * @brief This methods reads network statistics from /proc/net/dev and returns them for each interface in a
    * separate map
    * @return a map where each interface is mapping the according network statistics map.
    */
    NetworkMetrics readNetworkStats() override;

    /**
    * @return Returns the wall clock time of the system in nanoseconds.
    */
    uint64_t getWallTimeInNs() override;
};

}// namespace NES

#endif// NES_INCLUDE_MONITORING_UTIL_SYSTEM_RESOURCES_READER_HPP_
