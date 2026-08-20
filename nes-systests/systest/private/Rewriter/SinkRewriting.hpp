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
#include <Rewriter/NamePrefixer.hpp>
#include <Rewriter/RewriteContext.hpp>
#include <Rewriter/SqlParse.hpp>

namespace NES
{

/// What a declared sink contributes when it is inlined into a query.
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
    SinkRewriter(const RewriteContext& context, const PrefixedNames& names, const SinkByName& sinkByName);

    /// Replaces a query's sink, declared or written into the query, with an anonymous sink that writes to the candidate result file.
    /// The candidate is chosen per query, so two queries never write the same file.
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

    const RewriteContext& context; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    const PrefixedNames& names; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    const SinkByName& sinkByName; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
};

}
