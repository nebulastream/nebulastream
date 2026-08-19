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

#include <filesystem>

#include <Model/Expectation.hpp>
#include <Model/Verdict.hpp>

namespace NES::Systest
{

/// One result check: the file a query wrote its result to, and the rows the test expects in it.
struct ResultCheck
{
    std::filesystem::path resultFile;
    ExpectedResult expected;
    /// Whether a row of that file is one JSON object.
    /// The checker compares such a row as text, because splitting it on commas would cut it inside the object.
    bool rowsAreJson = false;
};

/// Compares a query's result file against the expected rows.
/// The schema in the file header aligns the fields, and each value compares by its type, with a tolerance for floats.
/// The check passes when the schemas and every tuple match.
[[nodiscard]] Verdict checkResult(const ResultCheck& check);

}
