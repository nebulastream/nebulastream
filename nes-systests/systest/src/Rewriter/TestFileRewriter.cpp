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

#include <Rewriter/TestFileRewriter.hpp>

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <Config/Config.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Discovery/TestFileReader.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileBuilder.hpp>
#include <Parser/TestFilePartition.hpp>
#include <Rewriter/ClassifiedStatement.hpp>
#include <Rewriter/Declarations.hpp>
#include <Rewriter/Emitter.hpp>
#include <Rewriter/NamePrefixer.hpp>
#include <Rewriter/RewriteContext.hpp>
#include <Util/Ranges.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// Re-checks what the config parser checks, for configs built in code.
const Host& getDefaultHost(const std::vector<Host>& allowed, const std::string_view option)
{
    PRECONDITION(not allowed.empty(), "Topology must list at least one worker in {} to assign a default host", option);
    return allowed.front();
}

/// Warns on a number the file lacks, likely a typo: `-t Filter.test:3,7` on queries 1-5 warns about 7.
void warnNumbersNotInFile(
    const std::unordered_set<SystestQueryId>& selectedNumbers, const ParsedTestFile& parsedFile, const std::filesystem::path& file)
{
    for (const auto number : selectedNumbers)
    {
        if (not std::ranges::any_of(
                parsedFile.statements, [&](const auto& statement) { return std::ranges::contains(getQueryNumbersOf(statement), number); }))
        {
            std::cerr << fmt::format(
                "Warning: Query number {} specified via command line argument but not found in file://{}\n", number, file.string());
        }
    }
}

}

TestFileRewriter::TestFileRewriter(const SystestConfiguration& config)
    : workingDir{config.workingDir.getValue()}
    , testDataDir{config.testDataDir.getValue()}
    , configDir{config.configDir.getValue()}
    , keyFactory{config.testDiscoverRoot.getValue()}
    , defaultSourceHost{getDefaultHost(config.clusterConfig.allowSourcePlacement, "allow_source_placement")}
    , defaultSinkHost{getDefaultHost(config.clusterConfig.allowSinkPlacement, "allow_sink_placement")}
{
}

ParsedTestFile TestFileRewriter::parse(const DiscoveredTestFile& testFile) const
{
    SystestParser parser;
    parser.registerSubstitutionRule({.keyword = "TESTDATA", .ruleFunction = [&](std::string& substitute) { substitute = testDataDir; }});
    parser.registerSubstitutionRule(
        {.keyword = "CONFIG/", .ruleFunction = [&](std::string& substitute) { substitute = (configDir / "").string(); }});
    parser.loadString(readTestFile(testFile.file));
    try
    {
        return buildTestFile(parser, testFile.file);
    }
    catch (Exception& exception)
    {
        tryLogCurrentException();
        exception.what() += fmt::format("Could not successfully parse test file://{}", testFile.file.string());
        throw;
    }
}

std::vector<RunnablePartition> TestFileRewriter::rewrite(const DiscoveredTestFile& testFile)
{
    const ParsedTestFile parsedFile = parse(testFile);
    warnNumbersNotInFile(testFile.queryFilter, parsedFile, testFile.file);

    /// Partition before selecting, so a query keeps the key (`JOIN_C1`) that it has in a full run.
    auto partitions = partitionByOverrides(parsedFile);
    std::vector<RunnablePartition> rewritten;
    rewritten.reserve(partitions.size());
    for (const auto& [partitionIdx, partition] : partitions | views::enumerate)
    {
        auto& [overrides, file] = partition;
        retainSelectedStatements(file.statements, testFile.queryFilter);
        if (not hasTestCases(file.statements))
        {
            continue;
        }
        /// The loop reads only the overrides after this move.
        auto runnable = rewritePartition(
            std::move(file),
            RewriteContext{
                .testFileKey = keyFactory.deriveKeyOf(testFile.file, partitionIdx, partitions.size()),
                .name = testFile.getName().getRawValue(),
                .workingDir = workingDir,
                .testDataDir = testDataDir,
                .sourceHost = defaultSourceHost,
                .sinkHost = defaultSinkHost});
        nameOwners.claim(runnable.originalNames, testFile.file);
        rewritten.push_back(RunnablePartition{.overrides = std::move(overrides), .file = std::move(runnable)});
    }
    return rewritten;
}

RunnableTestFile TestFileRewriter::rewritePartition(ParsedTestFile partition, const RewriteContext& context)
{
    auto classified = classifyStatements(std::move(partition));
    return Emitter{context, declareAll(classified, context.testFileKey)}.emit(std::move(classified));
}

}
