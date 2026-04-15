#include <nes-source-runtime-bindings/lib.h>
#include "Runtime/BufferRecycler.hpp"
#include "rust/cxx.h"

void on_error(const SourceContext& context, rust::String message)
{
    context.onError(message.c_str());
}

void on_data(
    const SourceContext& context,
    NES::detail::MemorySegment* segment,
    rust::Fn<void(rust::Box<AsyncCompletionContext> ctx, ::AsyncCompletionResult ret)> done,
    rust::Box<AsyncCompletionContext> ctx)
{
    context.onData(NES::fromRust(segment), done, std::move(ctx));
}

void on_eos(const SourceContext& context)
{
    context.onEoS();
}
