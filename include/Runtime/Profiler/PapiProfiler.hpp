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

#ifndef NES_INCLUDE_RUNTIME_PROFILER_PAPIPROFILER_HPP_
#define NES_INCLUDE_RUNTIME_PROFILER_PAPIPROFILER_HPP_
#include <Runtime/Profiler/BaseProfiler.hpp>
#include <fstream>
#include <memory>
#include <vector>
namespace NES {
namespace Runtime {
namespace Profiler {
#ifdef ENABLE_PAPI_PROFILER
/**
 * @brief This class samples hardware performance counters using PAPI
 * @experimental
 */
class PapiCpuProfiler : public BaseProfiler {
  public:
    enum class Presets {
        /// frontend, backend (core and memory), branch mispredicition stalls
        Multiplexing = 0,
        MultiplexExtended,
        /// compmemory bound
        MemoryBound,
        ResourceUsage,
        /// compute IPC
        IPC,
        /// compute instruction cache misses
        ICacheMisses,
        /// compute mem <-> cache bw :: not sure it works fine (add likwid)
        CacheBandwidth,
        /// compute data cache misses
        CachePresets,
        ///  compute prefetcher cache misses
        CachePrefetcherPresets,
        CachePresetsEx,
        /// compute misprediction
        BranchPresets,
        /// frontend related latency
        FrontendLatency,
        CachePrefetcherPresetsExt,
        CoreBound,
        InvalidPreset,
        /// details on L1 cache behaviour
        L1Detail
    };


    /**
     * @brief Creates a PapiCpuProfiler with a given preset (@see Presets)
     * @param preset the preset of events to check
     * @param csvWriter a file stream to write results in csv format
     * @param threadId thread identifier
     * @param coreId core identifier
     */
    explicit PapiCpuProfiler(Presets preset, std::ofstream&& csvWriter, uint32_t threadId, uint32_t coreId);

    virtual ~PapiCpuProfiler();

    /**
     * @brief start sampling the hardware performance counter preset
     * @return the tsc representing the moment we start sampling
     */
    uint64_t startSampling() override;

    /**
     * @brief stop sampling the hardware performance counter preset
     * @param numItems the number of items (records/buffers/...) processed from the moment we started
     * @return the tsc representing the moment we stop sampling
     */
    uint64_t stopSampling(std::size_t numItems) override;

  private:
    std::ofstream csvWriter;
    const uint32_t threadId;
    const uint32_t coreId;
    const double freqGhz;
    double startTsc;
    const Presets preset;

    std::vector<int> currEvents;
    std::vector<long long> currSamples;

    int eventSet;

    bool isStarted;
};

using PapiCpuProfilerPtr = std::shared_ptr<PapiCpuProfiler>;
#endif
}// namespace Profiler
}// namespace Runtime
}// namespace NES
#endif//NES_INCLUDE_RUNTIME_PROFILER_PAPIPROFILER_HPP_
