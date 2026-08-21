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

#include <Rewriter/PreparedStatement.hpp>

#include <memory>
#include <utility>
#include <variant>
#include <vector>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileParser.hpp>
#include <Rewriter/ParsedStatement.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// Reads what a CREATE declares from its parse tree, and binds attached data only to a physical source.
/// The test file format allows an ATTACH after any CREATE, because the parser matches line prefixes and never reads the statement.
/// This rejects a misplaced ATTACH.
ClassifiedCreate classify(const ParsedStatement& parsed, const Systest::CreateStatement& create)
{
    if (auto* physicalSource = findFirst<AntlrSQLParser::CreatePhysicalSourceDefinitionContext>(parsed.tree()))
    {
        return CreatePhysicalSourceStatement{.definition = physicalSource, .attached = create.attach};
    }
    if (create.attach.has_value())
    {
        throw TestException("ATTACH supplies the data of a physical source, but this statement declares something else: {}", create.sql);
    }

    if (auto* logicalSource = findFirst<AntlrSQLParser::CreateLogicalSourceDefinitionContext>(parsed.tree()))
    {
        return CreateLogicalSourceStatement{.definition = logicalSource};
    }
    if (auto* sink = findFirst<AntlrSQLParser::CreateSinkDefinitionContext>(parsed.tree()))
    {
        return CreateSinkStatement{.definition = sink};
    }
    if (auto* model = findFirst<AntlrSQLParser::CreateModelDefinitionContext>(parsed.tree()))
    {
        return CreateModelStatement{.definition = model};
    }
    throw TestException("Unsupported CREATE statement: {}", create.sql);
}

}

std::vector<PreparedStatement> prepare(const TestFile& testFile)
{
    std::vector<PreparedStatement> prepared;
    prepared.reserve(testFile.statements.size());
    for (const auto& statement : testFile.statements)
    {
        prepared.push_back(std::visit(
            Overloaded{
                [](const Systest::CreateStatement& create) -> PreparedStatement
                {
                    auto parsed = std::make_unique<ParsedStatement>(create.sql);
                    auto declaration = classify(*parsed, create);
                    return PreparedCreate{.create = &create, .parsed = std::move(parsed), .classified = std::move(declaration)};
                },
                [](const Systest::QueryStatement& query) -> PreparedStatement { return &query; },
                [](const Systest::DifferentialStatement& block) -> PreparedStatement { return &block; },
                [](const Systest::ExplainStatement& explain) -> PreparedStatement { return &explain; }},
            statement));
    }
    return prepared;
}

}
