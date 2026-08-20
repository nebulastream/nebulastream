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

/// The lines a query should produce, one string per line.
using ExpectedResult = std::vector<std::string>;

struct ExpectedRows
{
    ExpectedResult rows;
    /// Whether each row of the result file is one JSON object, which the checker reads whole rather than splitting into fields.
    /// The rewriter takes this from the sink it inlines, so a parsed test file always carries false here.
    bool rowsAreJson = false;
};

struct ExpectedError
{
    ErrorCode code;
    std::optional<std::string> message;
};

/// The plan an `EXPLAIN` should print.
struct ExpectedPlan
{
    ExpectedResult lines;
    /// The prefix the rewriter puts in front of every name it qualifies, stripped from the printed plan before comparing,
    /// because the expected lines hold the names the test file wrote.
    std::string qualifyingPrefix;
};

/// What a query should yield: result rows, a specific error, or a plan.
using Expectation = std::variant<ExpectedRows, ExpectedError, ExpectedPlan>;

}
