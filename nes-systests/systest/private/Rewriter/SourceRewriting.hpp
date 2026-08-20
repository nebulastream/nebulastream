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
#include <Model/RewrittenTest.hpp>
#include <Rewriter/ClassifiedStatement.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/RewriteTarget.hpp>
#include <Rewriter/SqlParse.hpp>

namespace NES
{

/// One physical source after rewriting: the statement to submit, and the file that the source's bytes come from, when they come from one.
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
    SourceRewriter(const RewriteTarget& target, const QualifiedNames& names);

    /// Builds the physical source statement to submit, plus the CSV to write when the test declared its rows inline.
    [[nodiscard]] RewrittenSource rewrite(SqlParse& parse, const PhysicalSourceDeclaration& declaration);

private:
    /// Builds the SET clause of a physical source.
    /// The clause pins the source to one worker unless the test chose one, so the coordinator can place it.
    /// It points at the file with the attached data when there is one, and defaults to a CSV input format unless the test chose one.
    /// The options that the test wrote come last, unchanged.
    [[nodiscard]] std::string setClauseFor(
        SqlParse& parse,
        AntlrSQLParser::NamedConfigExpressionSeqContext* declared,
        const std::optional<std::filesystem::path>& dataFile) const;

    const RewriteTarget& target;
    const QualifiedNames& names;
    size_t ordinal = 0;
};

/// Rewrites the data file path of a source written into a query to an absolute path under the test data directory.
/// A test gives that path relative to the test data directory, as it does everywhere else.
/// The worker resolves a relative path against its own working directory, so only an absolute path reaches the same file.
void makeAnonymousSourcePathsAbsolute(
    const SqlParse& parse, antlr4::TokenStreamRewriter& rewriter, const std::filesystem::path& testDataDir);

/// Completes a source that is written into a query with the options that every runnable source needs.
/// It pins the source to the run's source worker, because the coordinator resolves an omitted host to the worker that
/// answers, and a run placed on a topology answers none, so such a source would have nowhere to run.
/// It defaults the input format to CSV, because creating the source's descriptor rejects a source without one.
/// An option that the test wrote itself stays as written.
void completeAnonymousSources(const SqlParse& parse, antlr4::TokenStreamRewriter& rewriter, const Host& host);

/// Adds config options to a physical source statement the rewriter already emitted.
/// The new options merge into the statement's single `SET` clause, keeping the options already there.
/// The runner needs this for a value that is known only once the run started, such as the port that a data server bound.
std::string addSourceOptions(const std::string& sql, const std::vector<std::string>& options);

}
