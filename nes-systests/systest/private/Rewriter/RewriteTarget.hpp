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
#include <Rewriter/NameQualifier.hpp>

namespace NES
{

/// Context of one test file's rewrite.
/// Directories are absolute paths on the host running this process, so different workers can resolve them
/// independently from the location where their process was started.
/// The source host and the sink host are separate fields, because a run on a cluster may place sources and sinks
/// on different workers, respectively.
struct RewriteTarget
{
    /// Used to qualify the catalog-visible names
    TestFileKey testFileKey;
    /// This file's display name in failures and progress lines, which discovery chose so that two files that
    /// share a stem in different folders do not report the same.
    std::string displayName;
    /// Holds generated data and result files
    std::filesystem::path workingDir;
    /// Holds the input files for a source to consume
    std::filesystem::path testDataDir;
    /// Hosts that sources and sinks within this test file are placed on
    Host sourceHost;
    Host sinkHost;

    /// The file that one case's sink writes.
    /// The suffix distinguishes files of one test file, so no two queries overwrite each others results.
    [[nodiscard]] std::filesystem::path resultFile(const std::string_view suffix) const
    {
        return workingDir / fmt::format("{}_{}.csv", testFileKey.value(), suffix);
    }

    /// The CSV file that the rewriter plans for an ATTACH INLINE physical source.
    /// The ordinal keeps the generated file names unique within the test file.
    [[nodiscard]] std::filesystem::path sourceDataFile(const size_t ordinal) const
    {
        return workingDir / "sources" / fmt::format("{}_{}.csv", testFileKey.value(), ordinal);
    }
};

}
