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
#include <optional>
#include <string>
#include <vector>

#include <AntlrSQLParser.h>
#include <TokenStreamRewriter.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Rewriter/ClassifiedStatement.hpp>
#include <Rewriter/NamePrefixer.hpp>
#include <Rewriter/RewriteContext.hpp>
#include <Rewriter/SqlParse.hpp>

namespace NES
{

/// One physical source after rewriting.
/// The input file (if present) is what is read by the source.
struct RewrittenSource
{
    SetupStatement statement;
    std::optional<std::filesystem::path> inputFile;
};

/// Rewrites the physical source declarations of one test file.
/// Counts them, so each generated data file gets a unique name.
class SourceRewriter
{
public:
    SourceRewriter(const RewriteContext& context, const PrefixedNames& names);

    /// Rewrites one physical source and stages its attached data for the runner.
    /// Takes the declaration by value, because the attached rows move into the staged data.
    [[nodiscard]] RewrittenSource rewrite(SqlParse& parse, PhysicalSourceDeclaration declaration);

private:
    [[nodiscard]] std::string renderSetClauseFor(
        SqlParse& parse,
        AntlrSQLParser::NamedConfigExpressionSeqContext* declared,
        const std::optional<std::filesystem::path>& dataFile) const;

    const RewriteContext& context; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    const PrefixedNames& names; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    size_t ordinal = 0;
};

/// Makes the relative data file path of a source written into a query absolute, under the test data directory.
/// The worker resolves a relative path from its own working directory, which may not match where the test data is.
void makeAnonymousSourcePathsAbsolute(
    const SqlParse& parse, antlr4::TokenStreamRewriter& rewriter, const std::filesystem::path& testDataDir);

/// Adds the config to an anonymous source.
void completeAnonymousSources(const SqlParse& parse, antlr4::TokenStreamRewriter& rewriter, const Host& host);

struct SourceOption
{
    std::string group;
    std::string key;
    std::string value;
};

/// Adds config options to a physical source statement that the rewriter already emitted.
/// The runner needs this for the server port, which is known only once the run started.
/// An explicitly set option takes precedence over the default.
std::string addSourceOptions(const std::string& sql, const std::vector<SourceOption>& options);

}
