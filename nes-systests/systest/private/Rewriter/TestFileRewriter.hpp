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
#include <Identifiers/Identifiers.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Rewriter/NamePrefixer.hpp>
#include <Rewriter/RewriteContext.hpp>

namespace NES
{

/// Turns one discovered test file into the partitions that the binder takes.
/// It parses the file, splits it by the worker settings that its queries state, keeps the selected queries,
/// and rewrites each partition under its own key.
class TestFileRewriter
{
public:
    explicit TestFileRewriter(const SystestConfiguration& config);

    /// Throws when the file cannot be read or parsed, which the caller reports as one failed check for the file.
    [[nodiscard]] std::vector<RunnablePartition> rewrite(const DiscoveredTestFile& testFile);

    /// Rewrites the statements of one partition into the statements that the runner submits.
    /// Every test file of one invocation shares the catalog,
    /// and nearly every file declares a logical source named `stream`,
    /// so every declared name gets the file's key as a prefix.
    /// Each declared sink is inlined into the query that writes to it, with a result file of its own.
    ///
    /// Three phases: classify every CREATE, register every catalog-visible name,
    /// then rewrite and emit every statement in file order.
    /// Each phase takes the previous phase's output by value and moves what it keeps into its own output,
    /// so the statements and their expectations reach the runnable test file without being copied.
    [[nodiscard]] static RunnableTestFile rewritePartition(ParsedTestFile partition, const RewriteContext& context);

private:
    [[nodiscard]] ParsedTestFile parse(const DiscoveredTestFile& testFile) const;

    std::filesystem::path workingDir;
    std::filesystem::path testDataDir;
    std::filesystem::path configDir;
    DiscoveryRoot discoveryRoot;
    /// The worker for a statement that states no placement of its own.
    Host defaultSourceHost;
    Host defaultSinkHost;
    /// One registry for the whole invocation, so two files cannot claim the same prefixed name.
    PrefixedNameOwners nameOwners;
};

}
