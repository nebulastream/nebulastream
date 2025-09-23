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

#include <Phases/PipelineBuilder/PipelineBuilderState.hpp>
#include <set>
#include <tuple>
#include <iostream>
#include <magic_enum/magic_enum.hpp>

namespace NES
{

/// @brief A simple struct to represent a unique transition.
/// We use this as a key in our std::set.
struct FSMTransitionInfo
{
    State from;
    Event onEvent;
    State to;

    /// @brief Provide an ordering for std::set to correctly store unique transitions.
    bool operator<(const FSMTransitionInfo& other) const
    {
        return std::tie(from, onEvent, to) < std::tie(other.from, other.onEvent, other.to);
    }
};

/// @brief A singleton-like function to access a global set of used transitions.
/// This is thread-safe for initialization in C++11 and later.
inline std::set<FSMTransitionInfo>& getUsedTransitionsSet()
{
    static std::set<FSMTransitionInfo> usedTransitions;
    return usedTransitions;
}

/// @brief Prints all unique transitions that were recorded during the test run.
inline void printAllUsedTransitions()
{
    std::cout << "\n--- FSM Transition Coverage Report ---" << std::endl;
    std::cout << "The following " << getUsedTransitionsSet().size() << " unique transitions were used during the test suite run:" << std::endl;
    std::cout << "=====================================================" << std::endl;
    for (const auto& transition : getUsedTransitionsSet())
    {
        std::cout << "  " << magic_enum::enum_name(transition.from)
                  << " -> " << magic_enum::enum_name(transition.onEvent)
                  << " -> " << magic_enum::enum_name(transition.to) << std::endl;
    }
    std::cout << "=====================================================" << std::endl;
}

/// @brief An RAII helper class to ensure the report is printed at the end of a scope.
/// Simply create an instance of this at the beginning of your main test function.
struct FSMTransitionReporter
{
    ~FSMTransitionReporter()
    {
        printAllUsedTransitions();
    }
};

}// namespace NES