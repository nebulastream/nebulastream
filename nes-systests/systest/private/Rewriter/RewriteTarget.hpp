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

namespace NES
{

/// Context of one test file's rewrite.
/// The key qualifies the catalog-visible names, the working directory takes the generated data and result files, and the
/// test data directory holds the files that the test refers to.
/// Both directories are absolute paths on the host running this process, and a worker resolves them on its own, so a run
/// reaching workers in other processes needs those paths to reach the same files there.
/// The source host and the sink host are separate fields, because a run on a cluster may place sources and sinks
/// on different workers, respectively.
struct RewriteTarget
{
    std::string testFileKey;
    /// This file's display name in failures and progress lines, which discovery chose so that two files that
    /// share a stem in different folders do not report the same.
    std::string displayName;
    std::filesystem::path workingDir;
    std::filesystem::path testDataDir;
    Host sourceHost;
    Host sinkHost;

    /// The file that one case's sink writes.
    /// The suffix keeps the files of one test file apart, so no two queries write the same one.
    [[nodiscard]] std::filesystem::path resultFile(const std::string_view suffix) const
    {
        return workingDir / fmt::format("{}_{}.csv", testFileKey, suffix);
    }

    /// The CSV that the rewriter plans for a source whose rows the test declared inline.
    /// The ordinal keeps the generated file names unique within the test file.
    [[nodiscard]] std::filesystem::path sourceDataFile(const size_t ordinal) const
    {
        return workingDir / "sources" / fmt::format("{}_{}.csv", testFileKey, ordinal);
    }
};

}
