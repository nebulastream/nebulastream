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

#include <cassert>
#include <coroutine>
#include <cstddef>
#include <deque>
#include <mutex>

namespace NES
{

/// Counting semaphore for coroutines. `acquire()` returns an awaiter that resumes once
/// a permit is available; `release()` hands a permit back, resuming a waiter inline if
/// one is queued. Used to bound in-flight async work (e.g. the number of buffers a
/// source may have outstanding in the pipeline at once).
class AsyncSemaphore
{
public:
    explicit AsyncSemaphore(std::ptrdiff_t initial = 0) noexcept : count_(initial) { }

    AsyncSemaphore(const AsyncSemaphore&) = delete;
    AsyncSemaphore& operator=(const AsyncSemaphore&) = delete;

    /// Awaiter returned by acquire()
    class [[nodiscard]] Awaiter
    {
    public:
        explicit Awaiter(AsyncSemaphore& sem) noexcept : sem_(sem) { }

        bool await_ready() noexcept
        {
            /// Fast path: try to take a permit without suspending.
            std::lock_guard lk(sem_.mtx_);
            if (sem_.count_ > 0)
            {
                --sem_.count_;
                return true;
            }
            return false;
        }

        bool await_suspend(std::coroutine_handle<> h) noexcept
        {
            std::lock_guard lk(sem_.mtx_);
            /// Re-check under the lock: a release() could have raced in
            /// between await_ready() returning false and us getting here.
            if (sem_.count_ > 0)
            {
                --sem_.count_;
                return false; /// resume immediately, don't suspend
            }
            handle_ = h;
            sem_.waiters_.push_back(this);
            return true; /// actually suspend
        }

        void await_resume() noexcept { }

    private:
        friend class AsyncSemaphore;
        AsyncSemaphore& sem_;
        std::coroutine_handle<> handle_{};
    };

    Awaiter acquire() noexcept { return Awaiter{*this}; }

    /// Synchronous release. If a waiter is queued, the caller resumes it
    /// inline and therefore keeps driving that coroutine on its own thread.
    void release() noexcept
    {
        Awaiter* w = nullptr;
        {
            std::lock_guard lk(mtx_);
            if (!waiters_.empty())
            {
                w = waiters_.front();
                waiters_.pop_front();
            }
            else
            {
                ++count_;
                return;
            }
        }
        /// Resume OUTSIDE the lock. The releasing thread now drives the
        /// coroutine until it next suspends (or returns).
        assert(w && w->handle_);
        w->handle_.resume();
    }

private:
    std::mutex mtx_;
    std::ptrdiff_t count_;
    std::deque<Awaiter*> waiters_; /// FIFO; swap for LIFO if preferred
};

}
