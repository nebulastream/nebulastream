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
#include <unordered_map>

#include <AntlrSQLParser.h>

#include <Identifiers/Identifier.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/RewriteTarget.hpp>
#include <Rewriter/SqlParse.hpp>

namespace NES
{

/// A sink declaration that is stored for inlining the sink later.
struct SinkDefinition
{
    std::string type;
    std::string schema;
};

using SinkByName = std::unordered_map<Identifier, SinkDefinition>;

/// The result file is absent when the sink writes none (e.g., Checksum, Void).
struct RewrittenSink
{
    std::string sql;
    std::optional<std::filesystem::path> resultFile;
};

/// Returns the query sink, and rejects a query that writes into none or more than one.
/// A query needs exactly one, because the runner reads back a single result file per query.
AntlrSQLParser::SinkContext* requireSingleSink(const SqlParse& parse, const std::string& sql);

/// The sink side of one rewrite: inlines a sink into the query that writes to it, and builds the declarations that a
/// test file with an EXPLAIN submits.
class SinkRewriter
{
public:
    SinkRewriter(const RewriteTarget& target, const QualifiedNames& names, const SinkByName& sinkByName);

    /// Inlines the sink that a query writes into by converting into an anonymous sink.
    /// A declared sink contributes its schema, and always writes CSV.
    /// A sink written into the query keeps its options as the test wrote them.
    /// A sink that writes a result file takes the candidate file, so two queries never write the same one.
    /// A sink specifying its own `SINK`.`FILE_PATH` is rejected, because the checker reads the file the rewriter chose.
    [[nodiscard]] RewrittenSink
    inlineSink(SqlParse& parse, AntlrSQLParser::SinkContext* sink, const std::filesystem::path& candidateResultFile) const;

    /// Builds the declaration to submit for a declared sink of a test file that contains an EXPLAIN.
    /// A sink is validated as it is declared, so a File sink needs its mandatory options even though an EXPLAIN writes no row.
    [[nodiscard]] std::string declaredSinkStatement(SqlParse& parse, AntlrSQLParser::CreateSinkDefinitionContext* definition) const;

private:
    [[nodiscard]] RewrittenSink inlined(
        const std::string& type,
        AntlrSQLParser::NamedConfigExpressionSeqContext* declared,
        const std::string& trailingOptions,
        const std::filesystem::path& candidateResultFile) const;

    /// A non-owning view for the duration of one rewrite; the owner outlives every sub-rewriter it hands these to.

    const RewriteTarget& target; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    const QualifiedNames& names; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    const SinkByName& sinkByName; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
};

}
