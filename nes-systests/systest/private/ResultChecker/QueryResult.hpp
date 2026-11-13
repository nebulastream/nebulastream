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
#include <optional>
#include <string>
#include <vector>

namespace NES
{

/// Reads the rows of a result file, one per line.
/// The checker knows the schema of the rows from the bound plan, so it does not read it from the file. If the sink writes a header,
/// the file has to start with exactly that header, whatever the output format; a file with the header and no rows is a valid empty
/// result, but a file without it is not. Without a header, every line is a row and an empty file is an empty result.
/// A file that cannot be read, or that does not start with the expected header, yields the reason as text for the verdict.
[[nodiscard]] std::expected<std::vector<std::string>, std::string>
loadQueryResult(const std::filesystem::path& resultFilePath, const std::optional<std::string>& expectedHeader);

}
