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

#include <memory>
#include <optional>

#include <Identifiers/Identifiers.hpp>
#include <rust/cxx.h>
#include <QueryOptimizerConfiguration.hpp>

namespace NES::Bridge
{
struct CatalogSnapshot;
struct PlannedStatement;

/// The settings that every statement of one coordinator is planned with.
struct Planner
{
    QueryOptimizerConfiguration optimizerConfig;
    /// For a statement without a HOST clause; none means that the clause is required.
    std::optional<Host> defaultHost;
};

/// Throws when an optimizer option is invalid, so a coordinator with a wrong option does not start.
std::unique_ptr<Planner> make_planner(rust::Str optimizerConfig, rust::Str defaultHost);

/// Plans one SQL statement for the Rust coordinator.
/// The statement is parsed and bound, and a query is also optimized and placed on workers.
/// The catalog is only read from the snapshot.
/// A failure is returned in the `error` field of the result, so this function does not throw.
PlannedStatement plan_sql(const Planner& planner, const CatalogSnapshot& snapshot, rust::Str sql);
}
