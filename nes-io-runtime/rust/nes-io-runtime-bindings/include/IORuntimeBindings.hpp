#pragma once
#include <atomic>
#include <cstddef>
#include <memory>
#include <Identifiers/Identifiers.hpp>

struct ThreadInitializationContext
{
    explicit ThreadInitializationContext(const NES::Host& worker_id) : workerId(worker_id) { }

    std::atomic_size_t thread_id = 1;
    NES::Host workerId;
};

void init_thread(std::shared_ptr<ThreadInitializationContext> context);
