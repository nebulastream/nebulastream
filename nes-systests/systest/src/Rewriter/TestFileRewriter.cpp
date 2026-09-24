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
#include <ranges>
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

/// A selected number that the file does not have is most likely a typo in the invocation.
/// For `-t Filter.test:3,7` on a file with queries 1 to 5, query 3 runs and 7 is reported;
/// the run goes on with the rest.
void warnNumbersNotInFile(
    const std::unordered_set<SystestQueryId>& selectedNumbers, const ParsedTestFile& parsedFile, const std::filesystem::path& file)
{
    for (const auto number : selectedNumbers)
    {
        const auto inFile = std::ranges::any_of(
            parsedFile.statements, [&](const auto& statement) { return std::ranges::contains(getQueryNumbersOf(statement), number); });
        if (not inFile)
        {
            std::cerr << fmt::format(
                "Warning: Query number {} specified via command line argument but not found in file://{}\n", number, file.string());
        }
    }
}

}

TestFileRewriter::TestFileRewriter(const SystestConfiguration& config, PlacementResolver resolvePlacement)
    : workingDir{config.workingDir.getValue()}
    , testDataDir{config.testDataDir.getValue()}
    , configDir{config.configDir.getValue()}
    , discoveryRoot{config.testDiscoverRoot.getValue()}
    , resolvePlacement{std::move(resolvePlacement)}
{
}

ParsedTestFile TestFileRewriter::parse(const DiscoveredTestFile& testFile) const
{
    SystestParser parser;
    parser.registerSubstitutionRule({.keyword = "TESTDATA", .ruleFunction = [&](std::string& substitute) { substitute = testDataDir; }});
    parser.registerSubstitutionRule(
        {.keyword = "CONFIG/",
         .ruleFunction = [&](std::string& substitute)
         {
             substitute = configDir;
             if (!substitute.empty() && substitute.back() != '/')
             {
                 substitute.push_back('/');
             }
         }});
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
    /// A selection lists query numbers from the command line, and an empty selection means every query.
    const auto selectedNumbers = testFile.enabledQueries.value_or(std::unordered_set<SystestQueryId>{});
    warnNumbersNotInFile(selectedNumbers, parsedFile, testFile.file);

    /// Partition first, select second.
    /// A partition's key counts all partitions of the file
    /// (`JOIN` when it is the only one, `JOIN_C1` when it is the second),
    /// so selecting first could remove partitions and change the key of the query that is left,
    /// and the same query would then be rewritten differently alone than with its whole file.
    auto partitions = partitionByOverrides(parsedFile);
    std::vector<RunnablePartition> rewritten;
    rewritten.reserve(partitions.size());
    for (const auto& [partitionIdx, partition] : partitions | NES::views::enumerate)
    {
        auto& [overrides, file] = partition;
        retainSelectedStatements(file.statements, selectedNumbers);
        /// When the selection drops every test case of a partition, nothing runs, so nothing is rewritten or staged.
        if (not hasTestCases(file.statements))
        {
            continue;
        }
        /// The rewriter needs the hosts before it emits SQL, and asking for them registers the worker for these settings.
        const auto placement = resolvePlacement(overrides);
        if (not placement.has_value())
        {
            fmt::print("Skipping {} because it asks for worker settings the run cannot give it\n", testFile.getName().getRawValue());
            continue;
        }
        const auto partitionKey = discoveryRoot.keyOf(testFile.file, partitionIdx, partitions.size());
        /// The rewrite consumes the partition's statements.
        /// Only its overrides are read afterwards.
        auto runnable = rewritePartition(
            std::move(file),
            RewriteContext{
                .testFileKey = partitionKey,
                .name = testFile.getName().getRawValue(),
                .workingDir = workingDir,
                .testDataDir = testDataDir,
                .sourceHost = placement->sources,
                .sinkHost = placement->sinks});
        nameOwners.claim(runnable.originalNames, testFile.file);
        rewritten.push_back(RunnablePartition{.overrides = overrides, .test = std::move(runnable)});
    }
    return rewritten;
}

RunnableTestFile TestFileRewriter::rewritePartition(ParsedTestFile partition, const RewriteContext& context)
{
    auto classified = classifyStatements(std::move(partition));
    auto declarations = declareAll(classified, context.testFileKey);
    return Emitter{context, std::move(declarations)}.emit(std::move(classified));
}

}
