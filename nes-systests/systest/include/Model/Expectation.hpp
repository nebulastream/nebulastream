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

#include <optional>
#include <string>
#include <variant>
#include <vector>

#include <ErrorHandling.hpp>

namespace NES
{

/// The lines that a query should produce, one string per line: the output tuples of a query, or the plan of an `EXPLAIN`.
using ExpectedResult = std::vector<std::string>;

/// The rows that a query should produce, and how to read the file where they are written.
struct ExpectedRows
{
    ExpectedResult rows;
    /// Whether each row of the result file is one JSON object, which the checker reads whole rather than splitting into fields.
    /// The rewriter takes this from the sink that it inlines, so a parsed test file always carries false here.
    bool rowsAreJson = false;
};

/// The error that a query should fail with: the error code, and optionally a message to match against.
struct ExpectedError
{
    ErrorCode code;
    std::optional<std::string> message;
};

/// The plan that an `EXPLAIN` should print, and the prefix to strip from it before comparing.
/// The rewriter qualifies a name by prefixing the test file's key and an underscore, and the expected plan holds what the test wrote.
struct ExpectedPlan
{
    ExpectedResult lines;
    std::string qualifyingPrefix;
};

/// What a query should yield: result rows, a specific error, or a plan.
/// A differential block states no expectation, because it asserts only that its two queries agree.
using Expectation = std::variant<ExpectedRows, ExpectedError, ExpectedPlan>;

}
