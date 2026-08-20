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
/// The input file is the file whose bytes the source reads, when there is one.
struct RewrittenSource
{
    SetupStatement statement;
    std::optional<std::filesystem::path> inputFile;
};

/// Rewrites the physical source declarations of one test file.
/// It counts them, so each generated data file gets a unique name.
class SourceRewriter
{
public:
    SourceRewriter(const RewriteContext& context, const PrefixedNames& names);

    /// Rewrites one physical source and stages its attached data for the runner.
    /// Takes the declaration by value, because the attached rows move into the staged data.
    [[nodiscard]] RewrittenSource rewrite(SqlParse& parse, PhysicalSourceDeclaration declaration);

private:
    [[nodiscard]] std::string setClauseFor(
        SqlParse& parse,
        AntlrSQLParser::NamedConfigExpressionSeqContext* declared,
        const std::optional<std::filesystem::path>& dataFile) const;

    const RewriteContext& context; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    const PrefixedNames& names; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    size_t ordinal = 0;
};

/// Resolves the relative data file path of a source written into a query against the test data directory.
/// The worker resolves a relative path against its own working directory, which is not where the test data is.
void makeAnonymousSourcePathsAbsolute(
    const SqlParse& parse, antlr4::TokenStreamRewriter& rewriter, const std::filesystem::path& testDataDir);

/// Adds to a source written into a query the defaults that a declared physical source gets, unless the test set them.
void completeAnonymousSources(const SqlParse& parse, antlr4::TokenStreamRewriter& rewriter, const Host& host);

/// Adds config options to a physical source statement the rewriter already emitted.
/// The new options merge into the statement's single `SET` clause, keeping the options already there.
/// The runner needs this for a value that is known only once the run started, such as the port that a data server bound.
std::string addSourceOptions(const std::string& sql, const std::vector<std::string>& options);

}
