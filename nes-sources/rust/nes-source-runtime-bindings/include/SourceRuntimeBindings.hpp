#pragma once
#include <functional>

#include <absl/functional/any_invocable.h>
#include <rust/cxx.h>
#include "Runtime/TupleBuffer.hpp"


struct AsyncCompletionContext;
enum class AsyncCompletionResult : ::std::uint8_t;
enum class AsyncFunctionResult : ::std::uint8_t;

struct AsyncCompletionToken
{
    rust::Fn<void(rust::Box<AsyncCompletionContext>, AsyncCompletionResult)> done;
    rust::Box<AsyncCompletionContext> ctx;

    void operator()(AsyncCompletionResult result) && { done(std::move(ctx), result); }
};

struct DataEmitter
{
    virtual ~DataEmitter() = default;

    virtual AsyncFunctionResult onData(NES::TupleBuffer, size_t size, AsyncCompletionToken) = 0;
    virtual AsyncFunctionResult onError(std::string errorMessage,  AsyncCompletionToken) = 0;
    virtual AsyncFunctionResult onEoS(AsyncCompletionToken) = 0;
};

struct SourceContext
{
    std::unique_ptr<DataEmitter> emitter;
};

AsyncFunctionResult source_on_error(
    SourceContext& context,
    rust::String message,
    rust::Fn<void(rust::Box<AsyncCompletionContext> ctx, AsyncCompletionResult ret)> done,
    rust::Box<AsyncCompletionContext> ctx);
AsyncFunctionResult source_on_data(
    SourceContext& context,
    NES::detail::MemorySegment* segment,
    size_t size,
    rust::Fn<void(rust::Box<AsyncCompletionContext> ctx, AsyncCompletionResult ret)> done,
    rust::Box<AsyncCompletionContext> ctx);

AsyncFunctionResult source_on_eos(
    SourceContext& context,
    rust::Fn<void(rust::Box<AsyncCompletionContext> ctx, AsyncCompletionResult ret)> done,
    rust::Box<AsyncCompletionContext> ctx);
