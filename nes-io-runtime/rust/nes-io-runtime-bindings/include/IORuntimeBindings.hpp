#pragma once
#include <atomic>
#include <cstddef>
#include <memory>
#include <Identifiers/Identifiers.hpp>

struct ThreadInitializationContext
{
    ThreadInitializationContext(size_t runtime_idx, const NES::Host& worker_id) : runtimeIdx(runtime_idx), workerId(worker_id) { }

    size_t runtimeIdx = 0;
    std::atomic_size_t thread_id = 1;
    NES::Host workerId;
};

void init_thread(std::shared_ptr<ThreadInitializationContext> context);
