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

#include <rust/cxx.h>

namespace NES::Bridge
{
struct PlannedStatement;
struct PlanningTransaction;

/// Plans one SQL statement for the Rust coordinator.
/// The statement is parsed and bound, and a query is also optimized and placed on workers.
/// Catalog lookups go back to Rust through `ctx`, on the transaction of the request.
/// A failure is returned in the `error` field of the result, so this function does not throw.
PlannedStatement plan_sql(const PlanningTransaction& ctx, rust::Str sql, rust::Str optimizerConfig, rust::Str defaultHost);
}
