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
#include <ResultChecker/ResultChecker.hpp>

namespace NES
{

/// The shape every check has to fulfill: a const member producing a Verdict.
template <typename C>
concept Check = requires(const C& check) {
    { check.check() } -> std::same_as<Verdict>;
};

/// The closed set of checks the runner knows. A new check becomes usable by being added here,
/// which is also where the Check concept is enforced on it.
using AnyCheck = std::variant<ResultCheck, DifferentialCheck, ExplainCheck>;

namespace detail
{
template <typename... Ts>
consteval bool allAlternativesAreChecks(std::variant<Ts...>*)
{
    return (Check<Ts> and ...);
}
}

static_assert(detail::allAlternativesAreChecks(static_cast<AnyCheck*>(nullptr)));

/// The only path from a check to a Verdict the runner reports.
[[nodiscard]] inline Verdict runCheck(const AnyCheck& check)
{
    return std::visit([](const auto& concrete) { return concrete.check(); }, check);
}

}
