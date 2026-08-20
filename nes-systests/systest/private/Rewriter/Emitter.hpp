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
#include <vector>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Model/RewrittenTest.hpp>
#include <Rewriter/ClassifiedStatement.hpp>
#include <Rewriter/Declarations.hpp>
#include <Rewriter/RewriteTarget.hpp>
#include <Rewriter/SinkRewriting.hpp>
#include <Rewriter/SourceRewriting.hpp>
#include <Rewriter/SqlParse.hpp>

namespace NES
{

/// The emitting phase of one rewrite.
/// An instance emits exactly one test file, so no state reaches the next rewrite.
/// The declaring phase's declarations come in through the constructor and stays constant while emitting.
class Emitter
{
public:
    Emitter(const RewriteTarget& target, Declarations declarations);

    /// Emits the setup statements, then the cases, and returns the assembled test.
    [[nodiscard]] RewrittenTest emit(ClassifiedTest& classified) &&;

private:
    /// One statement's SQL after the full query rewrite, and what the rewrite learned about it.
    /// The query, differential and EXPLAIN paths each wrap this into their own case.
    struct RewrittenSql
    {
        std::string sql;
        std::optional<std::filesystem::path> resultFile;
        std::vector<std::filesystem::path> inputFiles;
    };

    /// The first pass: rewrites every CREATE into a setup statement, which also records the data file of each source,
    /// so a query emitted by the second pass can resolve its input files wherever its ATTACH stands in the file.
    void emitSetup(std::vector<ClassifiedStatement>& statements, bool submitsDeclaredSinks);
    /// The second pass: rewrites everything that is not a CREATE into a case, in file order.
    void emitCases(std::vector<ClassifiedStatement>& statements);

    [[nodiscard]] RewrittenSql rewriteSql(const std::string& sql, SystestQueryId id, std::string_view resultDiscriminator);
    void emitCreate(ClassifiedCreate& create, bool submitsDeclaredSinks);
    void emitExplain(const ExplainStatement& explain);
    void emitDifferential(const DifferentialStatement& block);

    /// Builds the model statement to submit, pointing at the file that holds the model.
    /// A test gives that path relative to the test data directory, but the worker loading it resolves a relative path against its own
    /// working directory, so the path has to be absolute.
    [[nodiscard]] PlainStatement modelStatement(SqlParse& parse, const ModelDeclaration& declaration) const;

    const RewriteTarget& target;
    Declarations declarations;
    SourceRewriter sourceRewriter;
    SinkRewriter sinkRewriter;

    /// The data files that each logical source reads, filled by the setup pass and read by the case pass.
    std::unordered_map<Identifier, std::vector<std::filesystem::path>> inputFilesBySource;
    RewrittenTest runnable;
};

}
