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

#include <ResultChecker/QueryResult.hpp>

#include <cstddef>
#include <expected>
#include <filesystem>
#include <fstream>
#include <ios>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>

namespace NES
{

std::expected<std::vector<std::string>, std::string>
loadQueryResult(const std::filesystem::path& resultFilePath, const std::optional<std::string>& expectedHeader)
{
    std::ifstream resultFile(resultFilePath);
    if (!resultFile)
    {
        return std::unexpected(fmt::format("result file was not written: {}", resultFilePath.string()));
    }

    if (expectedHeader.has_value())
    {
        /// The header ends with the tuple delimiter of its format, which need not be a newline, so it is compared byte for byte.
        std::string header(expectedHeader->size(), '\0');
        resultFile.read(header.data(), static_cast<std::streamsize>(header.size()));
        header.resize(static_cast<size_t>(resultFile.gcount()));
        if (header != *expectedHeader)
        {
            return std::unexpected(
                fmt::format(
                    "result file does not start with the header {:?} that the sink writes, but with {:?}: {}",
                    *expectedHeader,
                    header,
                    resultFilePath.string()));
        }
    }

    std::vector<std::string> rows;
    for (std::string line; std::getline(resultFile, line);)
    {
        rows.push_back(std::move(line));
    }
    return rows;
}

}
