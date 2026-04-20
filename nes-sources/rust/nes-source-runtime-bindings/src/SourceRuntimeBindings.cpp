#include <nes-source-runtime-bindings/lib.h>
#include "Runtime/BufferRecycler.hpp"
#include "rust/cxx.h"

AsyncFunctionResult source_on_error(
    SourceContext& context,
    rust::String message,
    rust::Fn<void(rust::Box<AsyncCompletionContext> ctx, ::AsyncCompletionResult ret)> done,
    rust::Box<AsyncCompletionContext> ctx)
{
    return context.emitter->onError(message.c_str(), AsyncCompletionToken{done, std::move(ctx)});
}

AsyncFunctionResult source_on_data(
    SourceContext& context,
    NES::detail::MemorySegment* segment,
    size_t size,
    rust::Fn<void(rust::Box<AsyncCompletionContext> ctx, ::AsyncCompletionResult ret)> done,
    rust::Box<AsyncCompletionContext> ctx)
{
    return context.emitter->onData(NES::fromRust(segment), size, AsyncCompletionToken{done, std::move(ctx)});
}

AsyncFunctionResult source_on_eos(
    SourceContext& context,
    rust::Fn<void(rust::Box<AsyncCompletionContext> ctx, ::AsyncCompletionResult ret)> done,
    rust::Box<AsyncCompletionContext> ctx)
{
    return context.emitter->onEoS(AsyncCompletionToken{done, std::move(ctx)});
}
