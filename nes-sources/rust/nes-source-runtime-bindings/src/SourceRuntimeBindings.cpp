#include <nes-source-runtime-bindings/lib.h>
#include "Runtime/BufferRecycler.hpp"
#include "rust/cxx.h"

void on_error(const SourceContext& context, rust::String message)
{
    context.onError(message.c_str());
}

void on_data(const SourceContext& context, NES::detail::MemorySegment* segment)
{
    context.onData(NES::fromRust(segment));
}

void on_eos(const SourceContext& context)
{
    context.onEoS();
}
