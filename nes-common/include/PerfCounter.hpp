//
// Created by ls on 5/28/24.
//

#ifndef PERFCOUNTER_HPP
#define PERFCOUNTER_HPP
#include <atomic>

struct PerfCounter {
    std::atomic<size_t> prepand;
    std::atomic<size_t> insert;
    std::atomic<size_t> found;
};

extern PerfCounter PerfCounterInstance;

#endif //PERFCOUNTER_HPP
