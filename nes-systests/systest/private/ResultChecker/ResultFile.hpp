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
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES::Systest
{

/// A parsed result file: the schema its header line declares, and the data rows that follow.
struct QueryResult
{
    Schema<UnqualifiedUnboundField, Ordered> schema;
    std::vector<std::string> result;
};

/// Reads a result file, taking the first line as the schema header and the rest as data rows.
/// Returns nullopt when the file cannot be opened or holds nothing.
[[nodiscard]] std::optional<QueryResult> loadQueryResult(const std::filesystem::path& resultFilePath);

}
