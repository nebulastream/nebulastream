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

#include <cstdint>
#include <string>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Model/Verdict.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

/// How two result sets differ: the schema difference and the row difference, each empty when that part matched.
struct ComparisonOutcome
{
    std::string schemaError;
    std::string resultError;
};

/// Compares an expected schema and rows against the actual ones, in any tuple order.
/// Field names and types align the schemas, and every value compares exactly except a float, which compares within a relative epsilon.
[[nodiscard]] ComparisonOutcome compare(
    const Schema<UnqualifiedUnboundField, Ordered>& expectedSchema,
    const std::vector<std::string>& expectedRows,
    const Schema<UnqualifiedUnboundField, Ordered>& actualSchema,
    const std::vector<std::string>& actualRows);

/// Which kind of comparison produced an outcome.
enum class ComparisonOrigin : uint8_t
{
    SingleQuery,
    DifferentialBlock
};

/// Turns a comparison outcome into a verdict.
[[nodiscard]] Verdict toVerdict(const ComparisonOutcome& outcome, ComparisonOrigin origin);

}
