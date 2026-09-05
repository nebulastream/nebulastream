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

/// A sink declaration that is stored for inlining.
struct SinkDefinition
{
    std::string type;
    std::string schema;
};

using SinkByName = std::unordered_map<Identifier, SinkDefinition>;

/// A sink after its lowering into the query that writes to it.
/// The result file is absent when the sink writes none.
struct RewrittenSink
{
    std::string sql;
    std::optional<std::filesystem::path> resultFile;
};

/// Returns the sink that a query writes into, and rejects a query that writes into none or more than one.
/// A query needs exactly one, because the runner reads back a single result file per query.
AntlrSQLParser::SinkContext* requireSingleSink(const SqlParse& parse, const std::string& sql);

/// The sink side of one rewrite: inlines a sink into the query that writes to it, and builds the declarations that a
/// test file with an EXPLAIN submits.
class SinkRewriter
{
public:
    SinkRewriter(const RewriteTarget& target, const QualifiedNames& names, const SinkByName& sinkByName);

    /// Lowers the sink that a query writes into, whether the test declared it above or wrote it into the query.
    /// Both forms become an anonymous sink of the declared type.
    /// A declared sink contributes its schema, because it sets the output field names and the casts to their types, and always writes CSV.
    /// A sink written into the query keeps its options as the test wrote them.
    /// A sink that writes a result file takes the candidate file, so two queries never write the same one.
    /// A sink that states its own `SINK`.`FILE_PATH` is rejected, because the checker reads the file the rewriter chose.
    [[nodiscard]] RewrittenSink
    inlineSink(SqlParse& parse, AntlrSQLParser::SinkContext* sink, const std::filesystem::path& candidateResultFile) const;

    /// Builds the declaration to submit for a declared sink of a test file that contains an EXPLAIN.
    /// The coordinator validates a sink as it is declared, so a File sink needs its mandatory options even though an EXPLAIN writes no row.
    [[nodiscard]] std::string declaredSinkStatement(SqlParse& parse, const AntlrSQLParser::CreateSinkDefinitionContext* definition) const;

private:
    /// Builds the anonymous sink replacing either form: the run's sink worker unless the sink chose its own host, then
    /// the result file and a CSV default when the sink writes a file and chose no format, then quoted strings for a
    /// checksum sink that did not choose, then the form's own trailing options.
    [[nodiscard]] RewrittenSink inlined(
        const std::string& type,
        const std::optional<std::string>& declaredFormat,
        bool hostAlreadySet,
        bool quoteStringsAlreadySet,
        const std::string& trailingOptions,
        const std::filesystem::path& candidateResultFile) const;

    const RewriteTarget& target;
    const QualifiedNames& names;
    const SinkByName& sinkByName;
};

}
