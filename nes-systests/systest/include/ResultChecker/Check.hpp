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
#include <variant>

#include <Model/Verdict.hpp>
#include <ResultChecker/DifferentialChecker.hpp>
#include <ResultChecker/ExplainChecker.hpp>
#include <ResultChecker/QueryResultChecker.hpp>

namespace NES
{

/// Checks are types that typically hold both the actual and the expected output of a test case.
/// They need to implement a member function that returns a Verdict.
template <typename C>
concept Check = requires(const C& check) {
    { check.check() } -> std::same_as<Verdict>;
};

/// The closed set of checks the runner knows.
/// A new check becomes usable by being added here.
using AnyCheck = std::variant<QueryResultCheck, DifferentialCheck, ExplainLinesCheck, ExplainRegexCheck>;

/// True when every alternative of a variant is a check, read from the variant itself so the list is not repeated.
template <typename>
constexpr bool AllAlternativesAreChecks = false;
template <typename... Ts>
constexpr bool AllAlternativesAreChecks<std::variant<Ts...>> = (Check<Ts> and ...);

static_assert(AllAlternativesAreChecks<AnyCheck>);

/// Conversion from a Check to a Verdict by calling `check()`.
[[nodiscard]] inline Verdict runCheck(const AnyCheck& check)
{
    return std::visit([](const auto& concrete) { return concrete.check(); }, check);
}

}
