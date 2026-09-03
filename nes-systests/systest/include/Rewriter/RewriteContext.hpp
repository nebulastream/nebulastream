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

#include <cstddef>
#include <filesystem>
#include <string>
#include <string_view>

#include <fmt/format.h>

#include <Identifiers/Identifiers.hpp>
#include <Rewriter/NamePrefixer.hpp>

namespace NES
{

/// Everything the rewrite of one test file needs to know about its surroundings.
/// Sources and sinks have separate hosts, because a cluster run may place them on different workers.
struct RewriteContext
{
    TestFileKey testFileKey;
    std::string name;
    std::filesystem::path workingDir;
    std::filesystem::path testDataDir;
    Host sourceHost;
    Host sinkHost;

    /// The suffix distinguishes the queries of one test file, so no two overwrite each other's results.
    /// For test file key `Filter` and suffix `3`: `<workingDir>/Filter_3.csv`.
    [[nodiscard]] std::filesystem::path resultFile(const std::string_view suffix) const
    {
        return workingDir / fmt::format("{}_{}.csv", testFileKey.value(), suffix);
    }

    /// CSV file that an ATTACH INLINE physical source reads.
    /// The ordinal distinguishes the inline sources of one test file: `<workingDir>/sources/Filter_0.csv`.
    [[nodiscard]] std::filesystem::path sourceDataFile(const size_t ordinal) const
    {
        return workingDir / "sources" / fmt::format("{}_{}.csv", testFileKey.value(), ordinal);
    }
};

}
