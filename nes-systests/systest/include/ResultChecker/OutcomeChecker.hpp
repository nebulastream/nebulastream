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

#include <chrono>
#include <expected>
#include <optional>
#include <span>
#include <string>

#include <DataTypes/UnboundField.hpp>
#include <Model/RewrittenTest.hpp>
#include <Model/Verdict.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// What running one statement produced, in the terms the checks need.
/// The reached state is the terminal state the workers reported, and the error in its place is what stopped the
/// statement from ever reaching one, which a test expecting an error compares against.
struct StatementOutcome
{
    std::expected<DistributedQueryStatusSnapshot, Exception> reached;

    /// The schema that the sink writes, which the result file is read back with.
    /// Absent when the statement produced no plan, and then there is no result file to read either.
    std::optional<Schema<UnqualifiedUnboundField, Ordered>> sinkOutputSchema;

    /// What an EXPLAIN printed. Such a statement is answered while it is compiled and never reaches a worker.
    std::optional<std::string> explained;

    /// The span between the query starting and stopping, as the workers recorded it.
    std::chrono::milliseconds execution{};
};

/// Checks the answers to one case against what the test expects.
/// A query answers with one outcome, and each kind of expectation is a different check.
/// A differential block answers with one outcome per half that ran, and its check is that the two results agree.
[[nodiscard]] Verdict checkCase(std::span<const StatementOutcome> outcomes, const RewrittenCase& testCase);

}
