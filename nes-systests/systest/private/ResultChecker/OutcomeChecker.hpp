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

#include <expected>
#include <optional>
#include <span>
#include <string>

#include <DataTypes/UnboundField.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/Verdict.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

struct StatementOutcome
{
    /// Final worker status, or the error that stopped the statement first.
    std::expected<DistributedQueryStatusSnapshot, Exception> reached;

    /// Result file schema, absent for a negative or EXPLAIN case where we don't produce any actual result tuples.
    std::optional<Schema<UnqualifiedUnboundField, Ordered>> sinkOutputSchema;

    std::optional<std::string> explained;
};

/// One outcome per query or EXPLAIN, one per executed differential half.
[[nodiscard]] Verdict
checkTestCase(std::span<const StatementOutcome> outcomes, const RewrittenTestCase& testCase, const OriginalNames& originalNames);

}
