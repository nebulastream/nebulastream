/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

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

    virtual AsyncFunctionResult onData(NES::TupleBuffer, AsyncCompletionToken) = 0;
    virtual AsyncFunctionResult onError(std::string errorMessage, AsyncCompletionToken) = 0;
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
    rust::Fn<void(rust::Box<AsyncCompletionContext> ctx, AsyncCompletionResult ret)> done,
    rust::Box<AsyncCompletionContext> ctx);

AsyncFunctionResult source_on_eos(
    SourceContext& context,
    rust::Fn<void(rust::Box<AsyncCompletionContext> ctx, AsyncCompletionResult ret)> done,
    rust::Box<AsyncCompletionContext> ctx);
