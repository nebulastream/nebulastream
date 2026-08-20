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
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifier.hpp>
#include <Model/Expectation.hpp>
#include <Model/RunnableTest.hpp>
#include <Model/SystestQueryId.hpp>
#include <Parser/SystestParser.hpp>
#include <Rewriter/Declarations.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/ParsedStatement.hpp>
#include <Rewriter/PreparedStatement.hpp>
#include <Rewriter/SinkRewriting.hpp>
#include <Rewriter/SourceRewriting.hpp>
#include <Rewriter/SqlRewriter.hpp>

namespace NES
{

/// The emitting phase of one rewrite.
/// An instance lives for exactly one test file, so no state carries over into the next rewrite.
/// The declaring phase's results come in through the constructor and stay constant while emitting.
class Emitter
{
public:
    Emitter(const RewriteTarget& target, Declarations declarations);


    [[nodiscard]] RunnableTest emit(std::vector<PreparedStatement>& prepared) &&;

private:
    [[nodiscard]] RunnableQuery
    rewriteQuery(const std::string& sql, SystestQueryId id, Expectation expected, std::string_view resultDiscriminator);
    /// Rewrites one CREATE statement and adds it to the setup statements.
    void emitCreate(PreparedCreate& prepared);
    void emitExplain(const Systest::ExplainStatement& explain);
    void emitDifferential(const Systest::DifferentialStatement& block);

    /// Builds the model statement to submit, pointing at the file that holds the model.
    /// A test gives that path relative to the test data directory, but the worker loading it resolves a relative path against its own
    /// working directory, so the path has to be absolute.
    [[nodiscard]] RunnableCreateStatement modelStatement(ParsedStatement& parsed, const CreateModelStatement& declaration) const;

    const RewriteTarget& target;
    QualifiedNames names;
    SinkByName sinkByName;
    /// Whether this test file submits its sink declarations, which a file that contains an EXPLAIN does.
    bool declaresSinks = false;
    SourceRewriter sourceRewriter;
    SinkRewriter sinkRewriter;

    /// The data files that each logical source reads.
    std::unordered_map<Identifier, std::vector<std::filesystem::path>> inputFilesBySource;
    RunnableTest runnable;
};

}
