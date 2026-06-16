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

#include <SourceRuntimeBindings.hpp>

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
    rust::Fn<void(rust::Box<AsyncCompletionContext> ctx, ::AsyncCompletionResult ret)> done,
    rust::Box<AsyncCompletionContext> ctx)
{
    return context.emitter->onData(NES::fromRust(segment), AsyncCompletionToken{done, std::move(ctx)});
}

AsyncFunctionResult source_on_eos(
    SourceContext& context,
    rust::Fn<void(rust::Box<AsyncCompletionContext> ctx, ::AsyncCompletionResult ret)> done,
    rust::Box<AsyncCompletionContext> ctx)
{
    return context.emitter->onEoS(AsyncCompletionToken{done, std::move(ctx)});
}
