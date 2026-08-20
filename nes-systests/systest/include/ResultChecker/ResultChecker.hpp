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

#include <DataTypes/UnboundField.hpp>
#include <Model/Expectation.hpp>
#include <Model/Verdict.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

/// One result check: the file that a query wrote its result to, the schema that the test declared for that file, and the rows that the
/// test expects in it.
struct ResultCheck
{
    std::filesystem::path resultFile;
    Schema<UnqualifiedUnboundField, Ordered> expectedSchema;
    ExpectedResult expected;

    /// Compares the query's result file against the declared schema and the expected rows.
    /// The schemas align the fields, and each value compares by its type, with a tolerance for floats.
    /// The check passes when the schemas and every tuple match.
    [[nodiscard]] Verdict check() const;
};

}
