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
class PapiCpuProfiler : public BaseProfiler {
  public:
    enum class Presets {
        Multiplexing = 0,
        MultiplexExtended,
        MemoryBound,
        ResourceUsage,
        IPC,
        ICacheMisses,
        CacheBandwidth,
        CachePresets,
        CachePrefetcherPresets,
        CachePresetsEx,
        BranchPresets,
        FrontendLatency,
        CachePrefetcherPresetsExt,
        CoreBound,
        InvalidPreset,
    };

    explicit PapiCpuProfiler(Presets preset, std::ofstream&& csvWriter, uint32_t threadId, uint32_t coreId);

    virtual ~PapiCpuProfiler();

    uint64_t startSampling() override;
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
