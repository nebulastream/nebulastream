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
#include <string>
#include <unordered_map>
#include <vector>
#include <fmt/base.h>
#include <magic_enum/magic_enum.hpp>

namespace NES
{

/// Action to perform on context
template <typename StateT, typename EventT, typename ContextT>
using ActionFn = std::function<void(ContextT&)>;

/// State machine transitions
template <typename StateT, typename EventT, typename ContextT>
struct Transition
{
    StateT from;
    EventT onEvent;
    StateT to;
    std::vector<ActionFn<StateT, EventT, ContextT>> actions;
};

template <typename StateT, typename EventT, typename ContextT>
class AbstractStateMachine
{
    using Key = std::pair<StateT, EventT>;
    using TransitionRow = Transition<StateT, EventT, ContextT>;

public:
    /// Get all state transitions in the state machine
    /// Returns a vector of all transitions that have been registered
    [[nodiscard]] std::vector<TransitionRow> getAllTransitions() const
    {
        std::vector<TransitionRow> allTransitions;
        allTransitions.reserve(tableMap.size());

        for (const auto& [key, transition] : tableMap)
        {
            allTransitions.push_back(transition);
        }

        return allTransitions;
    }

    [[nodiscard]] std::string getAllTransitionsAsString() const
    {
        return getAllTransitionsAsStringWithConverters(
            [](StateT state) { return magic_enum::enum_name(state); }, [](EventT event) { return magic_enum::enum_name(event); });
    }

    template <typename StateToStringFn, typename EventToStringFn>
    std::string getAllTransitionsAsStringWithConverters(StateToStringFn stateToString, EventToStringFn eventToString) const
    {
        std::string result = "State Machine Transitions:\n";
        result += "=============================\n";

        for (const auto& [key, transition] : tableMap)
        {
            result += "From: " + std::string(stateToString(transition.from))
                + " -> On Event: " + std::string(eventToString(transition.onEvent)) + " -> To: " + std::string(stateToString(transition.to))
                + " (Actions: " + std::to_string(transition.actions.size()) + ")\n";
        }

        return result;
    }

protected:
    AbstractStateMachine(std::vector<TransitionRow> table, StateT initial, StateT invalid)
        : currentState(initial), invalidState(invalid), initialState(initial)
    {
        for (auto&& row : std::move(table))
        {
            tableMap.emplace(Key{row.from, row.onEvent}, std::move(row));
        }
    }

    StateT step(EventT event, ContextT& ctx)
    {
        auto it = tableMap.find(Key{currentState, event});
        if (it == tableMap.end())
        {
            return invalidState;
        }

        currentState = it->second.to;
        for (auto& act : it->second.actions)
        {
            act(ctx);
        }
        return currentState;
    }

    void reset() { currentState = initialState; }

    [[nodiscard]] StateT getState() const { return currentState; }

private:
    StateT currentState;
    const StateT invalidState;
    const StateT initialState;
    std::unordered_map<Key, TransitionRow> tableMap;
};
}
