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
#include <filesystem>
#include <string>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

struct QueryResult
{
    Schema<UnqualifiedUnboundField, Ordered> schema;
    std::vector<std::string> tuples;
};

/// Reads a result file: the first line is the schema header, every further line one row.
/// A file with a header and no rows can be a valid empty result.
/// A file that cannot be read yields the reason as text for the verdict.
[[nodiscard]] std::expected<QueryResult, std::string> loadQueryResult(const std::filesystem::path& resultFilePath);

}
