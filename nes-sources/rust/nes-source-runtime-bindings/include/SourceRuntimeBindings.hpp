#pragma once
#include <functional>
#include <rust/cxx.h>
#include "Runtime/TupleBuffer.hpp"

struct SourceContext
{
    std::function<void(NES::TupleBuffer)> onData;
    std::function<void(std::string)> onError;
    std::function<void()> onEoS;
};

void on_error(const SourceContext& context, rust::String message);
void on_data(const SourceContext& context, NES::detail::MemorySegment* segment);
void on_eos(const SourceContext& context);
