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
#include <optional>
#include <string>

#include <AntlrSQLParser.h>

#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/ParsedStatement.hpp>
#include <Rewriter/SqlRewriter.hpp>

namespace NES
{

/// A sink after its lowering into the query that writes to it.
/// The result file is absent when the sink writes none.
/// The output format decides how the checker reads the rows back, so it comes along with the file.
struct InlinedSink
{
    std::string sql;
    std::optional<std::filesystem::path> resultFile;
    bool rowsAreJson = false;
};

/// Returns the sink that a query writes into, and rejects a query that writes into none or more than one.
/// A query needs exactly one, because the runner reads back a single result file per query.
AntlrSQLParser::SinkContext* requireSingleSink(const ParsedStatement& parsed, const std::string& sql);

/// The sink side of one rewrite: inlines a sink into the query that writes to it, and builds the declarations that a
/// test file with an EXPLAIN submits.
class SinkRewriter
{
public:
    SinkRewriter(const RewriteTarget& target, const QualifiedNames& names, const SinkByName& sinkByName);

    /// Lowers the sink that a query writes into, whether the test declared it above or wrote it into the query.
    /// Both forms become an anonymous sink of the declared type.
    /// A sink that writes a result file takes the candidate file, so two queries never write the same one.
    [[nodiscard]] InlinedSink
    inlineSink(ParsedStatement& parsed, AntlrSQLParser::SinkContext* sink, const std::filesystem::path& candidateResultFile) const;

    /// Builds the declaration to submit for a declared sink of a test file that contains an EXPLAIN.
    /// The coordinator validates a sink as it is declared, so a File sink needs its mandatory options even though an EXPLAIN writes no row.
    [[nodiscard]] std::string
    declaredSinkStatement(ParsedStatement& parsed, const AntlrSQLParser::CreateSinkDefinitionContext* definition) const;

private:
    /// Inlines a sink the test file declared into the query that writes to it, as an anonymous sink of the declared type.
    /// The declared schema has to come along, because it sets the output field names and the casts to their types.
    [[nodiscard]] InlinedSink inlineDeclaredSink(const SinkDefinition& declaration, const std::filesystem::path& candidateResultFile) const;

    /// Inlines a sink that the query wrote itself, keeping it anonymous and keeping its options as the test wrote them.
    /// The result file and the host come from here, and CSV only when the query chose no output format.
    /// The output schema stays out, because semantic analysis infers it from the query.
    [[nodiscard]] InlinedSink inlineQuerySink(
        ParsedStatement& parsed,
        const AntlrSQLParser::AnonymousSinkContext* anonymous,
        const std::filesystem::path& candidateResultFile) const;

    const RewriteTarget& target;
    const QualifiedNames& names;
    const SinkByName& sinkByName;
};

}
