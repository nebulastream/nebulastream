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
#include <typeindex>
#include <unordered_map>
#include <ErrorHandling.hpp>

namespace NES
{

namespace detail
{
/// Thread-local hook map shared with NES::Thread.
/// When NES::Thread creates a child, it snapshots this map and replays all hooks on the new thread.
/// Keyed by type_index so each WorkerLocalSingleton<T> gets exactly one slot.
extern thread_local std::unordered_map<std::type_index, std::function<void()>> threadInitHooks;

/// Set to true by NES::Thread on child threads. Used by WorkerLocalSingleton to produce
/// better diagnostics when instance() is called on a non-NES thread.
extern thread_local bool isNESThread;
}

/// CRTP base for worker-scoped singletons. Provides a thread-local instance pointer
/// that is automatically propagated to child threads created via NES::Thread.
///
/// Usage:
///   class MyService : public WorkerLocalSingleton<MyService> { ... };
///
/// Construction sets the thread-local and registers a hook. Destruction restores
/// the previous instance (stack semantics for nested scopes / tests).
template <typename T>
class WorkerLocalSingleton
{
    static thread_local T* _instance;
    T* _previous = nullptr;

    static void doRegister(T* self) noexcept
    {
        _instance = self;
        if (self)
        {
            detail::threadInitHooks[std::type_index(typeid(T))] = [self]() noexcept
            {
                _instance = self;
            };
        }
        else
        {
            detail::threadInitHooks.erase(std::type_index(typeid(T)));
        }
    }

public:
    WorkerLocalSingleton() noexcept
    {
        _previous = _instance;
        doRegister(static_cast<T*>(this));
    }

    ~WorkerLocalSingleton() noexcept { doRegister(_previous); }

    WorkerLocalSingleton(const WorkerLocalSingleton&) = delete;
    WorkerLocalSingleton& operator=(const WorkerLocalSingleton&) = delete;
    WorkerLocalSingleton(WorkerLocalSingleton&&) = delete;
    WorkerLocalSingleton& operator=(WorkerLocalSingleton&&) = delete;

    /// Returns the instance for the current thread. Fails if none is set.
    [[nodiscard]] static T& instance()
    {
        INVARIANT(
            _instance,
            "WorkerLocalSingleton<{}>::instance() called on a thread without an instance. {}",
            typeid(T).name(),
            detail::isNESThread ? "This is a NES thread but the singleton was not constructed before the thread was spawned."
                                : "This thread was not created via NES::Thread and does not have access to worker-scoped singletons.");
        return *_instance;
    }

    /// Returns the instance for the current thread, or nullptr if none is set.
    [[nodiscard]] static T* tryInstance() noexcept { return _instance; }
};

template <typename T>
thread_local T* WorkerLocalSingleton<T>::_instance = nullptr;

}
