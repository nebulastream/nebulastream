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

#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <Identifiers/Identifier.hpp>

/// The wire format for catalog source/sink config maps, shared by the write path (`SqlPlanner.cpp`, which encodes
/// `*Result` structs) and the read path (`PlannerContext.cpp`, which decodes catalog JSON back into descriptors).
/// Both paths must agree on two things: how config keys are serialized, and where a sink's formatter options
/// are nested. Defining them once here to prevent drift.
namespace NES::CatalogConfig
{

/// A sink's output-formatter options are stored nested under this key inside the sink config.
inline constexpr std::string_view OUTPUT_FORMATTER_KEY{"OUTPUT_FORMATTER"};

/// Config maps are keyed by `Identifier` in C++, but JSON/`rfl` can only carry string keys, so keys are serialized in
/// their canonical (case-folded) spelling. `toStringKeys` and `toIdentifierKeys` are inverses.
inline std::unordered_map<std::string, std::string> toStringKeys(const std::unordered_map<Identifier, std::string>& config)
{
    return config | std::views::transform([](const auto& kv) { return std::pair{kv.first.asCanonicalString(), kv.second}; })
        | std::ranges::to<std::unordered_map<std::string, std::string>>();
}

inline std::unordered_map<Identifier, std::string> toIdentifierKeys(const std::unordered_map<std::string, std::string>& config)
{
    return config | std::views::transform([](const auto& kv) { return std::pair{Identifier::fromCanonical(kv.first), kv.second}; })
        | std::ranges::to<std::unordered_map<Identifier, std::string>>();
}

}
