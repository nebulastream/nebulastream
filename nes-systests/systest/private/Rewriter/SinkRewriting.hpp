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

struct SinkDefinition
{
    std::string type;
    std::string schema;
};

using SinkByName = std::unordered_map<Identifier, SinkDefinition>;

/// The result file is absent for some sink types that occur in the tests (e.g., Checksum, Void).
struct RewrittenSink
{
    std::string sql;
    std::optional<std::filesystem::path> resultFile;
};

/// Returns the query sink from the AST, and rejects a query that declares less or more than one.
AntlrSQLParser::SinkContext* requireSingleSink(const SqlParse& parse, const std::string& sql);

/// Inlines the sink into the query statement, and rewrites the sink declarations of EXPLAIN test cases.
class SinkRewriter
{
public:
    SinkRewriter(const RewriteContext& context, const PrefixedNames& names, const SinkByName& sinkByName);

    /// Replaces a query's sink (declared or anonymous) with an anonymous sink.
    /// The candidate result file is chosen per query, so two queries never write the same file.
    [[nodiscard]] RewrittenSink
    inlineSink(SqlParse& parse, AntlrSQLParser::SinkContext* sink, const std::filesystem::path& candidateResultFile) const;

    /// Builds the declaration to submit for a declared sink of a test file that contains an EXPLAIN.
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
