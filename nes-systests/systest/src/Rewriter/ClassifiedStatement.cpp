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

#include <Rewriter/ClassifiedStatement.hpp>

#include <memory>
#include <utility>
#include <variant>

#include <Model/ParsedTestFile.hpp>
#include <Rewriter/SqlParse.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// Reads what a CREATE declares from its parse tree, and binds attached data only to a physical source.
/// The test file format allows an ATTACH after any CREATE, because the parser matches line prefixes and never reads the statement.
/// This rejects a misplaced ATTACH.
CreateDeclaration classify(const SqlParse& parse, const CreateStatement& create)
{
    if (auto* physicalSource = findFirst<AntlrSQLParser::CreatePhysicalSourceDefinitionContext>(parse.tree()))
    {
        return PhysicalSourceDeclaration{.definition = physicalSource, .attached = create.attach.has_value() ? &*create.attach : nullptr};
    }
    if (create.attach.has_value())
    {
        throw TestException("ATTACH supplies the data of a physical source, but this statement declares something else: {}", create.sql);
    }

    if (auto* logicalSource = findFirst<AntlrSQLParser::CreateLogicalSourceDefinitionContext>(parse.tree()))
    {
        return LogicalSourceDeclaration{.definition = logicalSource};
    }
    if (auto* sink = findFirst<AntlrSQLParser::CreateSinkDefinitionContext>(parse.tree()))
    {
        return SinkDeclaration{.definition = sink};
    }
    if (auto* model = findFirst<AntlrSQLParser::CreateModelDefinitionContext>(parse.tree()))
    {
        return ModelDeclaration{.definition = model};
    }
    throw TestException("Unsupported CREATE statement: {}", create.sql);
}

}

ClassifiedTest classifyStatements(const ParsedTestFile& testFile)
{
    ClassifiedTest classified;
    classified.statements.reserve(testFile.statements.size());
    for (const auto& statement : testFile.statements)
    {
        classified.statements.push_back(std::visit(
            Overloaded{
                [](const CreateStatement& create) -> ClassifiedStatement
                {
                    auto parse = std::make_unique<SqlParse>(create.sql);
                    auto declaration = classify(*parse, create);
                    return ClassifiedCreate{.create = &create, .parse = std::move(parse), .declaration = std::move(declaration)};
                },
                [](const SelectStatement& query) -> ClassifiedStatement { return &query; },
                [](const DifferentialStatement& block) -> ClassifiedStatement { return &block; },
                [&](const ExplainStatement& explain) -> ClassifiedStatement
                {
                    classified.containsExplain = true;
                    return &explain;
                }},
            statement));
    }
    return classified;
}

}
