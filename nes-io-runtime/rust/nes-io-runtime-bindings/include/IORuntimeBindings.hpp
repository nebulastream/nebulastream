#pragma once
#include <cstddef>
#include <memory>
#include <Identifiers/Identifiers.hpp>

struct ThreadInitializationContext
{
    size_t runtimeIdx = 0;
    NES::Host workerId;
};

void init_thread(std::shared_ptr<ThreadInitializationContext> context);
