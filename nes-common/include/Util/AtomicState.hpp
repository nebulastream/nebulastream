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

#include <concepts>
#include <functional>
#include <mutex>
#include <type_traits>
#include <utility>
#include <variant>

/// This is a thread-safe variant like data structure, that allows atomic transition from one state into the other state.
/// e.g. AtomicState<A,B> would allow a transition from A -> B by applying a function `B map(A&&)`
template <typename... States>
struct AtomicState
{
    /// Construct AtomicState from any valid State
    template <typename T>
    requires(std::same_as<std::remove_cvref_t<T>, States> || ...)
    explicit AtomicState(T&& state) : state(std::forward<T>(state))
    {
    }

    /// Attempts all transitions in order and stops at the first successful transition. If no transition was successful this function
    /// returns false.
    template <typename... TransitionFunctions>
    bool transition(const TransitionFunctions&... transitionFunctions)
    {
        std::scoped_lock lock(mutex);
        return (false || ... || tryTransitionLocked(std::function(transitionFunctions)));
    }

    /// Attempts a single transition. If the transition was successful this function returns true.
    template <typename FromState, typename ToState>
    bool transition(const std::function<ToState(FromState&&)>& transitionFunction)
    {
        std::scoped_lock lock(mutex);
        return tryTransitionLocked(transitionFunction);
    }

    template <typename State>
    bool is()
    {
        std::scoped_lock lock(mutex);
        return std::holds_alternative<State>(state);
    }

private:
    template <typename FromState, typename ToState>
    bool tryTransitionLocked(const std::function<ToState(FromState&&)>& transitionFunction)
    {
        if (!std::holds_alternative<FromState>(state))
        {
            return false;
        }
        state = transitionFunction(std::move(std::get<FromState>(state)));
        return true;
    }

    std::mutex mutex;
    std::variant<States...> state;
};
