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
#include <Model/ParsedTestFile.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Rewriter/ClassifiedStatement.hpp>
#include <Rewriter/Declarations.hpp>
#include <Rewriter/RewriteContext.hpp>
#include <Rewriter/SinkRewriting.hpp>
#include <Rewriter/SourceRewriting.hpp>
#include <Rewriter/SqlParse.hpp>

namespace NES
{

/// Turns one classified test file into the statements that the runner submits.
/// Two passes: every CREATE first, then every test case in file order.
/// An instance emits exactly one test file, so no state reaches the next rewrite.
class Emitter
{
public:
    Emitter(const RewriteContext& context, Declarations declarations);

    [[nodiscard]] RunnableTestFile emit(ClassifiedTestFile classified) &&;

private:
    struct RewrittenSql
    {
        std::string sql;
        std::vector<std::optional<std::filesystem::path>> resultFiles;
        std::vector<std::filesystem::path> inputFiles;
    };

    void emitSetup(std::vector<ClassifiedCreate> setup, bool submitsDeclaredSinks);
    void emitTestCases(std::vector<TestCaseStatement> testCases);

    [[nodiscard]] RewrittenSql emitSelect(const std::string& sql, SystestQueryId id, std::string_view resultDiscriminator);
    void emitCreate(ClassifiedCreate create, bool submitsDeclaredSinks);
    void emitQuery(SelectStatement query);
    void emitExplain(ExplainStatement explain);
    void emitDifferential(const DifferentialStatement& block);

    [[nodiscard]] PlainStatement modelStatement(SqlParse& parse, const ModelDeclaration& declaration) const;

    const RewriteContext& context; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    Declarations declarations;
    SourceRewriter sourceRewriter;
    SinkRewriter sinkRewriter;

    std::unordered_map<Identifier, std::vector<std::filesystem::path>> inputFilesBySource;
    RunnableTestFile runnable;
};

}
