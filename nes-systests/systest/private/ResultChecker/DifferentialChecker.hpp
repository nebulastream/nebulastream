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
#include <optional>
#include <string>

#include <DataTypes/UnboundField.hpp>
#include <Model/Verdict.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

/// One half of a differential block: the file its query wrote, the schema of its bound plan's sink, and the header that sink writes.
struct DifferentialSide
{
    std::filesystem::path resultFile;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    /// Absent if the sink writes no header.
    std::optional<std::string> expectedHeader;
};

/// One differential check: the two result files have to agree.
/// The files are parsed and compared tuple by tuple, the same way a result is compared with its expectation, so they
/// need not be byte-identical.
struct DifferentialCheck
{
    DifferentialSide first;
    DifferentialSide second;

    [[nodiscard]] Verdict check() const;
};

}
