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

#include <coroutine>
#include <exception>

namespace NES
{

/// Fire-and-forget coroutine used to drive a `coro::task<...>` chain to completion
/// from a synchronous call site -- typically a Rust FFI entry point that hands work
/// to async C++ and reports back via a completion callback.
///
/// `initial_suspend` is `suspend_never`, so the coroutine body runs eagerly on the
/// calling thread until it first suspends (e.g. on an AsyncSemaphore or a buffer that
/// is not yet available). `final_suspend` is `suspend_never`, so the coroutine frame
/// is destroyed when the body returns. The body must therefore own everything it needs
/// (captured by value as coroutine parameters) and must not throw -- an escaping
/// exception calls std::terminate.
struct DetachedTask
{
    struct promise_type
    {
        DetachedTask get_return_object() noexcept { return {}; }

        std::suspend_never initial_suspend() noexcept { return {}; }

        std::suspend_never final_suspend() noexcept { return {}; }

        void return_void() noexcept { }

        void unhandled_exception() noexcept { std::terminate(); }
    };
};

}
