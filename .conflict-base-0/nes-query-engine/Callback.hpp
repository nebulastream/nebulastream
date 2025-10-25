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

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <absl/functional/any_invocable.h>

namespace NES
{

struct CallbackOwner;
struct CallbackRef;

/// Callback system with synchronization guarantee:
/// When CallbackOwner is destroyed, either:
/// 1. The callback has fully completed execution, OR
/// 2. The callback will never run
/// The CallbackOwner destructor blocks until any in-flight callback completes.
/// Special case: If the owner is destroyed from within the callback itself,
/// we detect this (same thread) and skip waiting to avoid deadlock.
struct Callback
{
    std::mutex mutex;
    std::condition_variable executingCondition;
    bool cancelled = false;
    bool executing = false;
    std::thread::id executingThread;
    std::atomic<size_t> guards = 0;
    absl::AnyInvocable<void()> callback;

    /// Called when the last CallbackRef is destroyed (guards reaches 0)
    void tryExecute();

    /// Called by CallbackOwner destructor - blocks until safe
    void cancelAndWait();

    /// Create a Callback with owner and initial ref (guard count = 1)
    static std::pair<CallbackOwner, CallbackRef> create();
};

/// As long as CallbackRefs are alive the callback will not be called.
/// The last CallbackRef to go out of scope will execute the callback if it has not been disabled by the CallbackOwner.
/// It is safe for a Callback to delete the CallbackOwner.
/// It is unsafe for the Callback to create new CallbackRefs.
struct CallbackRef
{
    CallbackRef() = default;
    explicit CallbackRef(std::shared_ptr<Callback> ref);
    ~CallbackRef();

    CallbackRef(const CallbackRef& other);
    CallbackRef(CallbackRef&& other) noexcept;

    CallbackRef& operator=(const CallbackRef& other);
    CallbackRef& operator=(CallbackRef&& other) noexcept;

private:
    void decrementAndMaybeTrigger();

    std::shared_ptr<Callback> ref;
};

/// The CallbackOwner can set the Callback and control its lifetime. As long as the CallbackOwner is alive the Callback can trigger.
/// After the destructor of the CallbackOwner has completed the Callback is guaranteed to either have completed or will never trigger.
/// The destructor may block as the callback can potentially be executed concurrently.
struct CallbackOwner
{
    CallbackOwner() = default;
    explicit CallbackOwner(std::shared_ptr<Callback> ref);
    ~CallbackOwner();

    CallbackOwner(const CallbackOwner&) = delete;
    CallbackOwner& operator=(const CallbackOwner&) = delete;

    CallbackOwner(CallbackOwner&& other) noexcept;
    CallbackOwner& operator=(CallbackOwner&& other) noexcept;

    /// Set the callback. Must be called before any CallbackRef is destroyed.
    void setCallback(absl::AnyInvocable<void()> callback);

private:
    std::shared_ptr<Callback> ref;
};

}
