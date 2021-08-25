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

#ifndef NES_INCLUDE_RUNTIME_RUNTIMEMANAGER_HPP_
#define NES_INCLUDE_RUNTIME_RUNTIMEMANAGER_HPP_

#include <vector>
#include <memory>
#include <Runtime/Allocator/NumaRegionMemoryAllocator.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>

namespace NES::Runtime {

/**
 * @brief This class is responsible to look up OS/HW specs of the underlying hardware, e.g., numa.
 */
class RuntimeManager {

  public:

    struct NumaDescriptor;
    /**
     * @brief Descriptor for a single core
     */
    struct CpuDescriptor {
        friend struct NumaDescriptor;

      public:
         explicit CpuDescriptor() : coreId(-1), cpuId(-1) {
            // nop
        }

        explicit CpuDescriptor(uint16_t coreId, uint16_t cpuId) : coreId(coreId), cpuId(cpuId) {
            // nop
        }

        uint16_t getCoreId() const { return coreId; }

        uint16_t getCpuId() const { return cpuId; }

        friend bool operator<(const CpuDescriptor& lhs, const CpuDescriptor& rhs) { return lhs.cpuId < rhs.cpuId; }

        friend bool operator==(const CpuDescriptor& lhs, const CpuDescriptor& rhs) { return lhs.cpuId == rhs.cpuId; }

      private:
        uint16_t coreId;
        uint16_t cpuId;
    };

    /**
     * @brief Descriptor for a single numa node
     */
    struct NumaDescriptor {
      public:
        explicit NumaDescriptor(uint32_t node_id) : nodeId(node_id), physicalCpus() {
            // nop
        }

        /**
         * @brief Adds a new cpu/code
         * @param cpu
         * @param core
         */
        void addCpu(uint16_t cpu, uint16_t core) {
            if (physicalCpus.find(core) == physicalCpus.end()) {
                physicalCpus[core] = CpuDescriptor(core, cpu);
            } else {
                physicalCpus[core].cpuId = std::min<uint16_t>(physicalCpus[core].cpuId, cpu);
            }
        }

        const uint32_t nodeId;
        std::map<uint16_t, CpuDescriptor> physicalCpus;
    };


  public:
    explicit RuntimeManager();

    /**
     * @brief Returns the global posix-based memory allocator
     * @return
     */
    NesDefaultMemoryAllocatorPtr getGlobalAllocator() const;

    /**
     * @brief Returns the numa allocator for the numaNodeIndex-th numa node
     * @param numaNodeIndex
     * @return
     */
    NumaRegionMemoryAllocatorPtr getNumaAllactor(uint32_t numaNodeIndex) const;

    /**
     * @brief Provides the count of available numa nodes
     * @return
     */
    uint32_t getNumberOfNumaRegions() const;

  private:
    NesDefaultMemoryAllocatorPtr globalAllocator;

    std::vector<NumaRegionMemoryAllocatorPtr> numaRegions;

    std::vector<NumaDescriptor> cpuMapping;
    uint32_t numaNodesCount = 0;
    uint32_t numPhysicalCpus = 0;
};

}


#endif//NES_INCLUDE_RUNTIME_RUNTIMEMANAGER_HPP_
