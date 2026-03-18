#pragma once
#include <rust/cxx.h>

struct SinkContext
{
    std::function<void(std::string_view)> onError;
    std::function<void(size_t)> onFlush;
};

void on_error(const SinkContext& context, rust::String message);

void on_flush(const SinkContext& context, size_t epoch);

