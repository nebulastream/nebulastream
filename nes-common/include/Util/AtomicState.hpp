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
#include <mutex>
#include <utility>
#include <variant>

/// This is a thread-safe variant like data structure, that allows atomic transition from one state into the other state.
/// e.g. AtomicState<A,B> would allow a transition from A -> B by applying a function `B map(A&&)`
template <typename... States>
struct AtomicState
{
    template <typename T>
    explicit AtomicState(T&& state) : state(std::forward<T>(state))
    {
    }

    template <typename... Fn>
    bool transition(const Fn&... fn)
    {
        std::scoped_lock lock(mutex);
        return (false || ... || try_transition_locked(std::function(fn)));
    }

    template <typename From, typename To>
    bool transition(const std::function<To(From&&)>& fn)
    {
        std::scoped_lock lock(mutex);
        return try_transition_locked(fn);
    }

private:
    template <typename From, typename To>
    bool try_transition_locked(const std::function<To(From&&)>& fn)
    {
        if (!std::holds_alternative<From>(state))
        {
            return false;
        }
        state = fn(std::move(std::get<From>(state)));
        return true;
    }
    std::recursive_mutex mutex;
    std::variant<States...> state;
};
