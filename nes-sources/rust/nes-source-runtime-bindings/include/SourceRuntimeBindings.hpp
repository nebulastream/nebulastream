#pragma once
#include <functional>
#include <rust/cxx.h>
#include "Runtime/TupleBuffer.hpp"


struct AsyncCompletionContext;
enum class AsyncCompletionResult : ::std::uint8_t;

struct SourceContext
{
    std::function<void(
        NES::TupleBuffer, rust::Fn<void(rust::Box<AsyncCompletionContext>, AsyncCompletionResult)>, rust::Box<AsyncCompletionContext>)>
        onData;
    std::function<void(std::string)> onError;
    std::function<void()> onEoS;
};

void on_error(const SourceContext& context, rust::String message);
void on_data(
    const SourceContext& context,
    NES::detail::MemorySegment* segment,
    rust::Fn<void(rust::Box<AsyncCompletionContext> ctx, AsyncCompletionResult ret)> done,
    rust::Box<AsyncCompletionContext> ctx);
void on_eos(const SourceContext& context);
