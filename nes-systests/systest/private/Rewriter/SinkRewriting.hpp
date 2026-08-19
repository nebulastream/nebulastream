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
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <AntlrSQLParser.h>

#include <Identifiers/Identifier.hpp>
#include <Rewriter/NamePrefixer.hpp>
#include <Rewriter/RewriteContext.hpp>
#include <Rewriter/SqlParse.hpp>

namespace NES
{

/// The options that a test wrote on a sink, kept as text so that they outlive the parse of the statement that wrote them.
struct WrittenOptions
{
    std::string text;
    /// The group and the key of every option in the text.
    std::vector<std::pair<std::string, std::string>> names;

    /// Returns whether the test set the option, so a default is only added where it set nothing.
    [[nodiscard]] bool sets(std::string_view group, std::string_view key) const;
};

/// Returns the options as the test wrote them, or none when it wrote no option list.
WrittenOptions writtenOptions(SqlParse& parse, AntlrSQLParser::NamedConfigExpressionSeqContext* options);

/// What a declared sink contributes when it is inlined into a query.
struct SinkDefinition
{
    std::string type;
    std::string schema;
    /// Carried into every query that writes into the sink, so that, e.g., the worker the declaration places it on holds there too.
    WrittenOptions options;
};

using SinkByName = std::unordered_map<Identifier, SinkDefinition>;

/// The result file is absent when the sink writes none (e.g., Checksum, Void).
struct RewrittenSink
{
    std::string sql;
    std::optional<std::filesystem::path> resultFile;
};

/// Returns the query's sinks in the order it lists them, and rejects a query that writes into none.
/// The runner reads back one result file per sink and checks it against the result block at the same position.
std::vector<AntlrSQLParser::SinkContext*> requireSinks(const SqlParse& parse, const std::string& sql);

/// Returns whether the query lists one declared sink more than once, which the engine rejects.
/// Inlining would give each listing an anonymous sink of its own and hide that mistake from the engine.
bool listsADeclaredSinkTwice(const std::vector<AntlrSQLParser::SinkContext*>& sinks);

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
        const WrittenOptions& written,
        const std::string& schemaOption,
        const std::filesystem::path& candidateResultFile) const;

    const RewriteContext& context; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    const PrefixedNames& names; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    const SinkByName& sinkByName; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
};

}
