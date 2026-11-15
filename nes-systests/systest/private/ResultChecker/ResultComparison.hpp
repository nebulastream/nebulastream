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

#include <string>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Model/Verdict.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

/// Compares the expected schema and rows with the actual ones, in any row order.
/// Fields pair up by position and match by name and type.
/// Every value compares exactly except a float, which compares within a relative epsilon.
/// A mismatch holds the side by side report a developer reads when a test fails.
[[nodiscard]] Verdict compareResults(
    const Schema<UnqualifiedUnboundField, Ordered>& expectedSchema,
    const std::vector<std::string>& expectedRows,
    const Schema<UnqualifiedUnboundField, Ordered>& actualSchema,
    const std::vector<std::string>& actualRows);

}
