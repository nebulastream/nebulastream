/*
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

#ifndef NES_CORE_INCLUDE_MONITORING_RESOURCESREADER_ABSTRACTSYSTEMRESOURCESREADER_HPP_
#define NES_CORE_INCLUDE_MONITORING_RESOURCESREADER_ABSTRACTSYSTEMRESOURCESREADER_HPP_

#include <Monitoring/ResourcesReader/SystemResourcesReaderType.hpp>
#include <memory>
#include <string>
#include <unordered_map>

namespace NES::Monitoring {
class CpuMetricsWrapper;
class MemoryMetrics;
class NetworkMetricsWrapper;
class DiskMetrics;
class RegistrationMetrics;
class RuntimeMetrics;

/**
* @brief This is an abstract class so derived classes can collect basic system information
* Warning: Only Linux distributions are currently supported
*/
class AbstractSystemResourcesReader {
  public:
    //  -- Constructors --
    AbstractSystemResourcesReader();
    AbstractSystemResourcesReader(const AbstractSystemResourcesReader&) = default;
    AbstractSystemResourcesReader(AbstractSystemResourcesReader&&) = default;
    //  -- Assignment --
    AbstractSystemResourcesReader& operator=(const AbstractSystemResourcesReader&) = default;
    AbstractSystemResourcesReader& operator=(AbstractSystemResourcesReader&&) = default;
    //  -- dtor --
    virtual ~AbstractSystemResourcesReader() = default;

    /**
    * @brief This methods reads runtime system metrics that are used within NES (e.g., memory usage, cpu load).
    * @return A RuntimeMetrics object containing the metrics.
    */
    virtual RuntimeMetrics readRuntimeNesMetrics();

    /**
    * @brief This methods reads system metrics that are used within NES (e.g., totalMemoryBytes, core num.).
    * @return A NodeRegistrationMetrics object containing the metrics.
    */
    virtual RegistrationMetrics readRegistrationMetrics();

    /**
    * @brief This method reads CPU information.
    * Warning: Does not return correct values in containerized environments.
    * @return A CpuMetricsWrapper object containing a vector<CpuMetrics> objects.
    */
    virtual CpuMetricsWrapper readCpuStats();

    /**
    * @brief This method reads memory information from the operating system.
    * Warning: Does not return correct values in containerized environments.
    * @return A map with the memory information
    */
    virtual MemoryMetrics readMemoryStats();

    /**
    * @brief This method reads disk stats from statvfs
    * Warning: Does not return correct values in containerized environments.
    * @return A map with the disk stats
    */
    virtual DiskMetrics readDiskStats();

    /**
    * @brief This methods reads network statistics for each interface and returns a wrapper object containing this information.
    * @return A NetworkMetricsWrapper object containing the network statistics for each available interface.
    */
    virtual NetworkMetricsWrapper readNetworkStats();

    /**
     * @brief Getter for the wall clock time.
    * @return Returns the wall clock time of the system in nanoseconds.
    */
    virtual uint64_t getWallTimeInNs();

    /**
     * @brief Getter for the reader type.
     * @return The SystemResourcesReaderType
     */
    [[nodiscard]] SystemResourcesReaderType getReaderType() const;

  protected:
    SystemResourcesReaderType readerType;
};
using AbstractSystemResourcesReaderPtr = std::shared_ptr<AbstractSystemResourcesReader>;

}// namespace NES::Monitoring

#endif// NES_CORE_INCLUDE_MONITORING_RESOURCESREADER_ABSTRACTSYSTEMRESOURCESREADER_HPP_
