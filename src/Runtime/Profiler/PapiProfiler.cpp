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

#ifdef ENABLE_PAPI_PROFILER
#include <Runtime/Profiler/PAPIProfiler.hpp>
#include <Util/Logger.hpp>
#include <papi.h>
#endif
#ifndef __x86_64__
#error "Unsupported Architecture"
#endif
namespace NES::Runtime::Profiler {
#ifdef ENABLE_PAPI_PROFILER
namespace detail {

static inline size_t rdtsc() {
    uint64_t rax;
    uint64_t rdx;
    asm volatile("rdtsc" : "=a"(rax), "=d"(rdx));
    return static_cast<size_t>((rdx << 32) | rax);
}

static double measureRdtscFreq() {
    struct timespec start, end;
    clock_gettime(CLOCK_REALTIME, &start);
    uint64_t rdtsc_start = rdtsc();

    // Do not change this loop! The hardcoded value below depends on this loop
    // and prevents it from being optimized out.
    uint64_t sum = 5;
    for (uint64_t i = 0; i < 1000000; i++) {
        sum += i + (sum + i) * (i % sum);
    }
    NES_ASSERT(sum == 13580802877818827968ull, "Error in RDTSC freq measurement");

    clock_gettime(CLOCK_REALTIME, &end);
    uint64_t clock_ns =
        static_cast<uint64_t>(end.tv_sec - start.tv_sec) * 1000000000 + static_cast<uint64_t>(end.tv_nsec - start.tv_nsec);
    uint64_t rdtsc_cycles = rdtsc() - rdtsc_start;

    double freq_ghz = rdtsc_cycles * 1.0 / clock_ns;
    NES_ASSERT(freq_ghz >= 0.5 && freq_ghz <= 5.0, "Invalid RDTSC frequency");

    return freq_ghz;
}

double toMsec(size_t cycles, double freqGhz) { return (cycles / (freqGhz * 1000000)); }

/**
 * @brief Utility class to bootstrap the PAPI library using RAII and static
 */
class PapiInitializer {
  public:
    /**
     * @brief this ctor initialize the library to use multiplexing and MT
     */
    PapiInitializer() {
        NES_ASSERT2_FMT(PAPI_library_init(PAPI_VER_CURRENT) == PAPI_VER_CURRENT,
                        "Failed to load PAPI " << PAPI_VERSION << "!=" << PAPI_VER_CURRENT);
        NES_ASSERT2_FMT(PAPI_multiplex_init() == PAPI_OK, "Failed to init multiplexing PAPI");
        NES_ASSERT2_FMT(PAPI_thread_init(static_cast<unsigned long (*)()>(pthread_self)) == PAPI_OK,
                        "Failed to init thread PAPI");
    }

    ~PapiInitializer() { PAPI_shutdown(); }
};
static PapiInitializer papi;
}// namespace detail

PapiCpuProfiler::PapiCpuProfiler(Presets preset, std::ofstream&& csvWriter, uint32_t threadId, uint32_t coreId)
    : csvWriter(std::move(csvWriter)), threadId(threadId), coreId(coreId), freqGhz(detail::measureRdtscFreq()),
      startTsc(detail::rdtsc()), preset(preset), eventSet(PAPI_NULL), isStarted(false) {
    auto err = PAPI_register_thread();
    NES_ASSERT2_FMT(err == PAPI_OK, "Cannot register thread on PAPI worker err=" << err);
    err = PAPI_create_eventset(&eventSet);
    NES_ASSERT2_FMT(err == PAPI_OK, "Cannot create PAPI event err=" << err);
    switch (preset) {
        case Presets::BranchPresets: {
            currEvents = {PAPI_BR_MSP, PAPI_BR_INS, PAPI_BR_TKN, PAPI_BR_NTK};
            currSamples.resize(currEvents.size(), 0);
            this->csvWriter << "core_id,numRecords,worker_id,ts,PAPI_BR_MSP,PAPI_BR_INS,PAPI_BR_TKN,PAPI_BR_NTK,"
                               "mispred_per_record\n";
            err = PAPI_add_events(eventSet, currEvents.data(), currEvents.size());
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot register events on PAPI reason: " << err);
            break;
        }
        case Presets::CachePresets: {
            currEvents = {PAPI_L1_TCM, PAPI_L2_TCM, PAPI_L3_TCM, PAPI_L3_TCA, PAPI_L1_DCM, PAPI_L1_ICM};
            currSamples.resize(currEvents.size(), 0);
            this->csvWriter << "core_id,numRecords,worker_id,ts,PAPI_L1_TCM,PAPI_L2_TCM,"
                               "PAPI_L3_TCM,PAPI_L3_TCA,PAPI_L1_DCM,PAPI_L1_ICM,"
                               "l1_misses_per_record,l2_misses_per_record,l3_misses_per_record,"
                               "records_per_l1_miss,records_per_l2_miss,records_per_l3_miss,"
                               "l1i_misses_per_record";
            err = PAPI_add_events(eventSet, currEvents.data(), currEvents.size());
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot register events on PAPI reason: " << err);
            break;
        }
        case Presets::CachePrefetcherPresets: {
            //            currEvents = {PAPI_PRF_DM, PAPI_CA_SHR, PAPI_CA_ITV, PAPI_CA_CLN, PAPI_TOT_INS, PAPI_TOT_CYC};
            currEvents = {PAPI_PRF_DM, PAPI_CA_SHR, PAPI_CA_ITV, PAPI_CA_CLN};
            currSamples.resize(currEvents.size(), 0);
            this->csvWriter << "core_id,numRecords,worker_id,ts,PAPI_PRF_DM,PAPI_CA_SHR,PAPI_CA_ITV,PAPI_CA_CLN\n";
            err = PAPI_add_events(eventSet, currEvents.data(), currEvents.size());
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot register events on PAPI reason: " << err);
            break;
        }
        case Presets::IPC: {
            currEvents = {PAPI_TOT_INS, PAPI_TOT_CYC, PAPI_REF_CYC};
            currSamples.resize(currEvents.size(), 0);
            this->csvWriter << "core_id,numRecords,worker_id,ts,PAPI_TOT_INS,PAPI_TOT_CYC,PAPI_REF_CYC,"
                               "ipc,cpi,instr_per_record,cycles_per_record\n";
            err = PAPI_add_events(eventSet, currEvents.data(), currEvents.size());
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot register events on PAPI reason: " << err);
            break;
        }
        case Presets::ResourceUsage: {// PAPI_RES_STL PAPI_MEM_WCY PAPI_PRF_DM PAPI_STL_CCY PAPI_REF_CYC
            currEvents = {PAPI_RES_STL, PAPI_MEM_WCY, PAPI_PRF_DM, PAPI_STL_CCY, PAPI_REF_CYC};
            currSamples.resize(currEvents.size(), 0);
            this->csvWriter
                << "core_id,numRecords,worker_id,ts,PAPI_RES_STL,PAPI_MEM_WCY,PAPI_PRF_DM,PAPI_STL_CCY,PAPI_REF_CYC\n";
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot register events on PAPI reason: " << err);
            break;
        }
        case Presets::CachePresetsEx: {
            currEvents = {PAPI_TLB_DM, PAPI_L3_TCM, PAPI_TLB_IM};
            currSamples.resize(currEvents.size(), 0);
            this->csvWriter << "core_id,numRecords,worker_id,ts,PAPI_TLB_DM,PAPI_L3_TCM,PAPI_TLB_IM,"
                               "l3_misses_per_record,tldb_misses_per_record,"
                               "itlb_misses_per_record\n";
            err = PAPI_add_events(eventSet, currEvents.data(), currEvents.size());
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot register events on PAPI reason: " << err);
            break;
        }
        case Presets::ICacheMisses: {
            currEvents = {PAPI_L2_ICM, PAPI_L1_ICM, PAPI_TLB_IM};
            currSamples.resize(currEvents.size(), 0);
            this->csvWriter << "core_id,numRecords,worker_id,ts,PAPI_L2_ICM,PAPI_L1_ICM,PAPI_TLB_IM,"
                               "l1i_misses_per_record,l2i_misses_per_record,itlb_misses_per_record\n";
            err = PAPI_add_events(eventSet, currEvents.data(), currEvents.size());
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot register events on PAPI reason: " << err);
            break;
        }
        case Presets::MemoryBound: {
            // EXE_ACTIVITY:BOUND_ON_STORES L1D_PEND_MISS:FB_FULL:c=1 CPU_CLK_THREAD_UNHALTED  MEM_LOAD_RETIRED.L1_MISS MEM_LOAD_RETIRED.L2_HIT MEM_LOAD_RETIRED.FB_HIT CYCLE_ACTIVITY.STALLS_L1D_MISS CYCLE_ACTIVITY.STALLS_L2_MISS CYCLE_ACTIVITY.STALLS_L3_MISS
            err = PAPI_assign_eventset_component(eventSet, 0);// 0 is cpu component
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot assign PAPI event set to component 0 worker={} with err {}" << err);
            err = PAPI_set_multiplex(eventSet);
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot promote PAPI event set to multiplex worker={} with err {}" << err);
            std::array events = {"CYCLE_ACTIVITY.STALLS_L1D_MISS",
                                 "CYCLE_ACTIVITY.STALLS_L2_MISS",
                                 "CYCLE_ACTIVITY.STALLS_L3_MISS",
                                 "L1D_PEND_MISS:FB_FULL:c=1",
                                 "MEM_LOAD_RETIRED.L2_HIT",
                                 "MEM_LOAD_RETIRED.FB_HIT",
                                 "MEM_LOAD_RETIRED.L1_MISS",
                                 "CPU_CLK_THREAD_UNHALTED",
                                 "CYCLE_ACTIVITY.STALLS_MEM_ANY",
                                 "EXE_ACTIVITY:BOUND_ON_STORES",
                                 "CYCLE_ACTIVITY:STALLS_L3_MISS"};
            currSamples.resize(events.size(), 0);
            this->csvWriter << "core_id,numRecords,worker_id,ts";
            for (const auto& event_name : events) {
                auto err = PAPI_add_named_event(eventSet, event_name);
                NES_ASSERT2_FMT(err == PAPI_OK,
                                "Cannot register event {} on PAPI worker={}: error={}" << event_name << " err=" << err);
                this->csvWriter << "," << event_name;
            }
            this->csvWriter << ",l1_bound,l2_bound,l3_bound,store_bound,dram_bound\n";
            break;
        }
        case Presets::L1Detail: {
            // EXE_ACTIVITY:BOUND_ON_STORES L1D_PEND_MISS:FB_FULL:c=1 CPU_CLK_THREAD_UNHALTED  MEM_LOAD_RETIRED.L1_MISS MEM_LOAD_RETIRED.L2_HIT MEM_LOAD_RETIRED.FB_HIT CYCLE_ACTIVITY.STALLS_L1D_MISS CYCLE_ACTIVITY.STALLS_L2_MISS CYCLE_ACTIVITY.STALLS_L3_MISS
            err = PAPI_assign_eventset_component(eventSet, 0);// 0 is cpu component
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot assign PAPI event set to component 0 worker={} with err {}" << err);
            err = PAPI_set_multiplex(eventSet);
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot promote PAPI event set to multiplex worker={} with err {}" << err);
            std::array events = {
                //
                //                "perf::PERF_COUNT_HW_CACHE_L1D.READ"
                //                                                 "perf::PERF_COUNT_HW_CACHE_L1D:WRITE"
                //                "perf::L1-DCACHE-PREFETCHES"
                "perf::PERF_COUNT_HW_CACHE_L1D:PREFETCH:MISS"
                //                "perf::PERF_COUNT_HW_CACHE_L1D:PREFETCH:MISS"

                //                                 "perf::PERF_COUNT_HW_CACHE_L1D.PREFETCH",
                //                                 "perf::PERF_COUNT_HW_CACHE_L1D.ACCESS"
                //                                 "perf::PERF_COUNT_HW_CACHE_L1D.MISS"
            };
            currSamples.resize(events.size(), 0);
            this->csvWriter << "core_id,numRecords,worker_id,ts";
            for (const auto& event_name : events) {
                auto err = PAPI_add_named_event(eventSet, event_name);
                NES_ASSERT2_FMT(err == PAPI_OK,
                                "Cannot register event {} on PAPI worker={}: error={}" << event_name << " err=" << err);
                this->csvWriter << "," << event_name;
            }
            this->csvWriter << ",READ,WRITE,PREFETCH,ACCESS,MISS\n";
            break;
        }
        case Presets::CachePrefetcherPresetsExt: {
            std::array events = {"L2_RQSTS:PF_MISS", "L2_RQSTS:PF_HIT", "L2_RQSTS:ALL_PF"};
            currSamples.resize(events.size(), 0);
            this->csvWriter << "core_id,numRecords,worker_id,ts";
            err = PAPI_assign_eventset_component(eventSet, 0);// 0 is cpu component
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot assign PAPI event set to component 0 worker={} with err {}" << err);
            err = PAPI_set_multiplex(eventSet);
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot promote PAPI event set to multiplex worker={} with err {}" << err);
            for (const auto& event_name : events) {
                auto err = PAPI_add_named_event(eventSet, event_name);
                NES_ASSERT2_FMT(err == PAPI_OK,
                                "Cannot register event {} on PAPI worker={}: error={}" << event_name << " err=" << err);
                this->csvWriter << "," << event_name;
            }
            this->csvWriter << "\n";
            break;
        }
        case Presets::FrontendLatency: {
            std::array events = {
                "IDQ_UOPS_NOT_DELIVERED:CYCLES_0_UOPS_DELIV_CORE",
                "FRONTEND_RETIRED:IDQ_1_BUBBLE",
                "FRONTEND_RETIRED:IDQ_2_BUBBLES",
                "UOPS_ISSUED.ANY",
                "UOPS_RETIRED:RETIRE_SLOTS",
                "CPU_CLK_THREAD_UNHALTED",
                "ICACHE_16B:IFDATA_STALL",
                "ICACHE_16B:IFDATA_STALL:e=1:c=1",
                "ICACHE_64B.IFTAG_STALL",
                "IDQ_UOPS_NOT_DELIVERED.CORE",
                "UOPS_EXECUTED:THREAD",
                "UOPS_EXECUTED:THREAD_CYCLES_GE_1",
            };
            currSamples.resize(events.size(), 0);
            this->csvWriter << "core_id,numRecords,worker_id,ts";
            err = PAPI_assign_eventset_component(eventSet, 0);// 0 is cpu component
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot assign PAPI event set to component 0 worker={} with err {}" << err);
            err = PAPI_set_multiplex(eventSet);
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot promote PAPI event set to multiplex worker={} with err {}" << err);
            for (const auto& event_name : events) {
                auto err = PAPI_add_named_event(eventSet, event_name);
                NES_ASSERT2_FMT(err == PAPI_OK,
                                "Cannot register event {} on PAPI worker={}: error={}" << event_name << " err=" << err);
                this->csvWriter << "," << event_name;
            }
            this->csvWriter << ",frontend_latency,frontend_bound,frontend_bandwidth,icache_misses,itlb_misses,ilp\n";
            break;
        }
        case Presets::CacheBandwidth: {
            // LONGEST_LAT_CACHE:MISS OFFCORE_REQUESTS:ALL_REQUESTS L2_LINES_IN:ALL L1D:REPLACEMENT
            NES_ASSERT2_FMT(PAPI_add_named_event(eventSet, "LONGEST_LAT_CACHE:MISS") == PAPI_OK,
                            "Cannot register events on PAPI worker={}");
            NES_ASSERT2_FMT(PAPI_add_named_event(eventSet, "OFFCORE_REQUESTS:ALL_REQUESTS") == PAPI_OK,
                            "Cannot register events on PAPI worker={}");
            NES_ASSERT2_FMT(PAPI_add_named_event(eventSet, "L2_LINES_IN:ALL") == PAPI_OK,
                            "Cannot register events on PAPI worker={}");
            NES_ASSERT2_FMT(PAPI_add_named_event(eventSet, "L1D:REPLACEMENT") == PAPI_OK,
                            "Cannot register events on PAPI worker={}");
            currSamples.resize(4, 0);
            this->csvWriter
                << "core_id,numRecords,worker_id,ts,LONGEST_LAT_CACHE:MISS,OFFCORE_REQUESTS:ALL_REQUESTS,L2_LINES_IN:ALL,"
                   "L1D:REPLACEMENT,fill_l1d_bw,fill_l2_bw,fill_l3_bw,access_l3_bw\n";
            break;
        }
        case Presets::CoreBound: {
            // EXE_ACTIVITY:EXE_BOUND_0_PORTS EXE_ACTIVITY:1_PORTS_UTIL EXE_ACTIVITY:2_PORTS_UTIL UOPS_RETIRED:RETIRE_SLOTS

            break;
        }
        case Presets::MultiplexExtended: {// skylake only
            // L1D_PEND_MISS:FB_FULL:c=1 CPU_CLK_THREAD_UNHALTED  MEM_LOAD_RETIRED.L1_MISS MEM_LOAD_RETIRED.L2_HIT MEM_LOAD_RETIRED.FB_HIT CYCLE_ACTIVITY.STALLS_L1D_MISS CYCLE_ACTIVITY.STALLS_L2_MISS CYCLE_ACTIVITY.STALLS_L3_MISS
            err = PAPI_assign_eventset_component(eventSet, 0);// 0 is cpu component
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot assign PAPI event set to component 0 worker={} with err {}" << err);
            err = PAPI_set_multiplex(eventSet);
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot promote PAPI event set to multiplex worker={} with err {}" << err);
            std::array events = {"CYCLE_ACTIVITY.STALLS_L1D_MISS",
                                 "CYCLE_ACTIVITY.STALLS_L2_MISS",
                                 "CYCLE_ACTIVITY.STALLS_L3_MISS",
                                 "L1D_PEND_MISS:FB_FULL:c=1",
                                 "MEM_LOAD_RETIRED.L2_HIT",
                                 "MEM_LOAD_RETIRED.FB_HIT",
                                 "MEM_LOAD_RETIRED.L1_MISS",
                                 "CPU_CLK_THREAD_UNHALTED",
                                 "CYCLE_ACTIVITY.STALLS_MEM_ANY"};
            currSamples.resize(events.size(), 0);
            this->csvWriter << "core_id,numRecords,worker_id,ts";
            for (const auto& event_name : events) {
                auto err = PAPI_add_named_event(eventSet, event_name);
                NES_ASSERT2_FMT(err == PAPI_OK,
                                "Cannot register event {} on PAPI worker={}: error={}" << event_name << " err=" << err);
                this->csvWriter << "," << event_name;
            }
            this->csvWriter << ",l1_bound,l2_bound,l3_bound\n";
            break;
        }
        case Presets::Multiplexing: {                         // skylake only
            err = PAPI_assign_eventset_component(eventSet, 0);// 0 is cpu component
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot assign PAPI event set to component 0 worker={} with err {}" << err);
            err = PAPI_set_multiplex(eventSet);
            NES_ASSERT2_FMT(err == PAPI_OK, "Cannot promote PAPI event set to multiplex worker={} with err {}" << err);
            std::array events = {
                "IDQ_UOPS_NOT_DELIVERED.CORE",    "UOPS_ISSUED.ANY",
                "UOPS_RETIRED.RETIRE_SLOTS",      "INT_MISC.RECOVERY_CYCLES",
                "BR_MISP_RETIRED.ALL_BRANCHES",   "MACHINE_CLEARS.COUNT",
                "CYCLE_ACTIVITY.STALLS_MEM_ANY",  "EXE_ACTIVITY.BOUND_ON_STORES",
                "EXE_ACTIVITY.EXE_BOUND_0_PORTS", "EXE_ACTIVITY.1_PORTS_UTIL",
                "EXE_ACTIVITY.2_PORTS_UTIL",      "CPU_CLK_THREAD_UNHALTED",
#ifdef EXTENDEND_PAPI_MULTIPLEXING
                "CYCLE_ACTIVITY.STALLS_L1D_MISS", "CYCLE_ACTIVITY.STALLS_L2_MISS",
                "CYCLE_ACTIVITY.STALLS_L3_MISS",  "L1D_PEND_MISS:FB_FULL:c=1",
                "MEM_LOAD_RETIRED.L2_HIT",        "MEM_LOAD_RETIRED.FB_HIT",
                "MEM_LOAD_RETIRED.L1_MISS",
#endif
            };
            currSamples.resize(events.size(), 0);
            this->csvWriter << "core_id,numRecords,worker_id,ts";
            for (const auto& event_name : events) {
                auto err = PAPI_add_named_event(eventSet, event_name);
                NES_ASSERT2_FMT(err == PAPI_OK,
                                "Cannot register event {} on PAPI worker={}: error={}" << event_name << " err=" << err);
                this->csvWriter << "," << event_name;
            }
            this->csvWriter << ",frontend_bound,bad_speculation,branch_mispred,retiring,"
                               "backend_bound,upc,core_bound_cycles,backend_bound_cycles,"
                               "memory_bound_fraction,memory_bound,core_bound,port0,port1,port2"
#ifdef EXTENDEND_PAPI_MULTIPLEXING
                            << ",l1_bound,l2_bound,l3_bound\n";
#else
                            << "\n";
#endif
            break;
        }
        default: {
            NES_ASSERT2_FMT(false, "Not supported");
            break;
        }
    }
    this->csvWriter << std::endl;
}

PapiCpuProfiler::~PapiCpuProfiler() {
    csvWriter.close();
    auto err = PAPI_cleanup_eventset(eventSet);
    NES_ASSERT2_FMT(err == PAPI_OK, "Cannot remove PAPI event set with err= " << err);
    err = PAPI_destroy_eventset(&eventSet);
    NES_ASSERT2_FMT(err == PAPI_OK, "Cannot destroy PAPI event set with err=" << err);
    NES_ASSERT2_FMT(PAPI_unregister_thread() == PAPI_OK, "Cannot unregister thread on PAPI worker=" << threadId);
}

uint64_t PapiCpuProfiler::startSampling() {
    NES_ASSERT2_FMT(PAPI_start(eventSet) == PAPI_OK, "Cannot start PAPI sampling on worker=" << threadId);
    isStarted = true;
    return (startTsc = detail::rdtsc());
}

uint64_t PapiCpuProfiler::stopSampling(std::size_t numRecords) {
    if (!isStarted) {
        return -1;
    }
    NES_ASSERT2_FMT(PAPI_stop(eventSet, currSamples.data()) == PAPI_OK, "Cannot stop PAPI sampling on worker=" << threadId);
    if ((numRecords < 1 || !isStarted)) {
        isStarted = false;
        return -1;
    }
    auto currentTsc = detail::rdtsc();
    double ts = detail::toMsec(currentTsc - startTsc, freqGhz);
    csvWriter << coreId << "," << numRecords << "," << threadId << "," << ts;
    for (auto value : currSamples) {
        csvWriter << "," << value;
    }
    switch (preset) {
        case Presets::BranchPresets: {
            double mispredPerRecord = numRecords == 0 ? 0 : currSamples[0] / double(numRecords);
            csvWriter << "," << mispredPerRecord;
            break;
        }
        case Presets::CachePresets: {
            double l1MissesPerRecord = numRecords == 0 ? 0 : currSamples[4] / double(numRecords);
            double l2MissesPerRecord = numRecords == 0 ? 0 : currSamples[1] / double(numRecords);
            double l3MissesPerRecord = numRecords == 0 ? 0 : currSamples[2] / double(numRecords);
            double l1iMissesPerRecord = numRecords == 0 ? 0 : currSamples[5] / double(numRecords);
            csvWriter << "," << l1MissesPerRecord << "," << l2MissesPerRecord << "," << l3MissesPerRecord;
            csvWriter << "," << (1. / l1MissesPerRecord) << "," << (1. / l2MissesPerRecord) << "," << (1. / l3MissesPerRecord)
                      << "," << l1iMissesPerRecord;
            break;
        }
        default: {
            NES_ASSERT2_FMT(false, "Not supported");
            break;
        }
    }
    csvWriter << std::endl;
    return currentTsc;
}
#endif
}// namespace NES::Runtime::Profiler
