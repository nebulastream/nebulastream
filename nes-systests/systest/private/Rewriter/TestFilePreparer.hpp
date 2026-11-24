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
#include <vector>

#include <Config/Config.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Rewriter/NamePrefixer.hpp>
#include <Rewriter/RewriteContext.hpp>
#include <Runner/Cluster.hpp>

namespace NES
{

/// Turns a test file into runnable partitions in several stages: parse -> split by worker settings -> select cases -> rewrite.
/// E.g., `Join.test` with queries under two worker settings becomes partitions keyed `JOIN_C0` and `JOIN_C1`,
/// while a file with one setting keeps `JOIN`.
class TestFilePreparer
{
public:
    /// Throws if the topology of the run has no worker or an empty allow list.
    explicit TestFilePreparer(const SystestConfiguration& config);

    /// Throws when the file cannot be read or parsed.
    [[nodiscard]] std::vector<RunnablePartition> prepare(const DiscoveredTestFile& testFile);

    /// Prefixes each declared name with the key, e.g., `stream` becomes `JOIN_STREAM`, because files with equal overrides share a catalog.
    [[nodiscard]] static RunnableTestFile rewritePartition(ParsedTestFile partition, const RewriteContext& context);

private:
    [[nodiscard]] ParsedTestFile parse(const DiscoveredTestFile& testFile) const;

    std::filesystem::path workingDir;
    std::filesystem::path testDataDir;
    std::filesystem::path configDir;
    TestFileKeyFactory keyFactory;
    /// Every cluster of the run registers the workers of the topology file, so all files get the same hosts.
    Placement placement;
    /// Shared across the run, so two files cannot claim the same prefixed name.
    PrefixedNameOwners nameOwners;
};

}
